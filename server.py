#!/usr/bin/env python3
"""MCP server for the Packform distributor API.

Exposes the Packform order lifecycle to an MCP client (Hermes Agent) as a set of
read-only tools. The server performs no writes against Packform.

Authentication
--------------
Two strategies, tried in order:

1. ``postgres``  - read the current bearer token from the table that the existing
   n8n "Access Token Save" workflow refreshes. Table and column names are
   configurable so no schema assumptions are baked in.
2. ``direct``    - authenticate against Packform directly with a username and
   password from the environment, caching the token in memory.

Set ``PACKFORM_AUTH_MODE`` to ``postgres``, ``direct`` or ``auto`` (default).
In ``auto`` the server tries Postgres first and falls back to direct auth.

Business rules
--------------
This server deliberately contains NO business rules. Margin thresholds, hard
stops and round logic live in the n8n workflows. The only computation here is
arithmetic (margin percentages), which is returned alongside the raw figures so
the agent never has to do the maths itself.
"""

from __future__ import annotations

import json
import os
import time
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel, ConfigDict, Field, field_validator

mcp = MCPServer("packform_mcp")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

API_BASE_URL = os.environ.get("PACKFORM_BASE_URL", "https://www.packform.com").rstrip("/")
REQUEST_TIMEOUT = float(os.environ.get("PACKFORM_MCP_TIMEOUT", "30"))

AUTH_MODE = os.environ.get("PACKFORM_AUTH_MODE", "auto").strip().lower()

# Postgres token source. Column names are configurable because the table was
# created by a third party and the schema is not ours to assume.
PG_DSN = os.environ.get("PACKFORM_PG_DSN", "")
PG_TOKEN_TABLE = os.environ.get("PACKFORM_TOKEN_TABLE", "access_token")
PG_TOKEN_COLUMN = os.environ.get("PACKFORM_TOKEN_COLUMN", "access_token")
PG_TOKEN_ORDER_BY = os.environ.get("PACKFORM_TOKEN_ORDER_BY", "")

# Direct auth fallback.
PACKFORM_USERNAME = os.environ.get("PACKFORM_USERNAME", "")
PACKFORM_PASSWORD = os.environ.get("PACKFORM_PASSWORD", "")

# Writes are not implemented. The flag exists so that the posture is explicit
# and so a future build can gate them rather than shipping them on by default.
ALLOW_WRITES = os.environ.get("PACKFORM_MCP_ALLOW_WRITES", "false").strip().lower() == "true"

DEFAULT_MONTHS_TO_INCLUDE = int(os.environ.get("PACKFORM_PAST_QUOTE_MONTHS", "3"))

_token_cache: Dict[str, Any] = {"token": None, "fetched_at": 0.0, "source": None}
TOKEN_TTL_SECONDS = float(os.environ.get("PACKFORM_TOKEN_TTL", "1800"))

# Why each auth source succeeded or failed on the last attempt. Surfaced by
# packform_health so a misconfiguration is diagnosable without shell access.
# Never contains a token or a connection string.
_diagnostics: Dict[str, str] = {}


class ResponseFormat(str, Enum):
    """Output format for tool responses."""

    MARKDOWN = "markdown"
    JSON = "json"


class PackformError(RuntimeError):
    """Raised when the server cannot satisfy a request itself."""


# --------------------------------------------------------------------------
# Authentication
# --------------------------------------------------------------------------


def _token_from_postgres() -> Optional[str]:
    """Read the most recent bearer token from the configured Postgres table.

    Returns None when Postgres is not configured, unreachable, or the table is
    empty. Never raises, so that ``auto`` mode can fall through to direct auth.
    The reason for any failure is recorded in ``_diagnostics`` so that
    packform_health can report it without exposing the connection string.
    """
    if not PG_DSN:
        _diagnostics["postgres"] = "PACKFORM_PG_DSN is not set, so the Postgres token source is disabled."
        return None
    try:
        import psycopg  # type: ignore
    except ImportError:
        _diagnostics["postgres"] = "The psycopg package is not installed; reinstall the server's dependencies."
        return None

    order_by = PG_TOKEN_ORDER_BY.strip()
    suffix = f" ORDER BY {order_by} DESC" if order_by else ""
    query = f'SELECT "{PG_TOKEN_COLUMN}" FROM {PG_TOKEN_TABLE}{suffix} LIMIT 1'
    try:
        with psycopg.connect(PG_DSN, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()
    except Exception as exc:
        detail = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
        name = type(exc).__name__
        if "OperationalError" in name or "connect" in detail.lower():
            _diagnostics["postgres"] = (
                f"Could not connect to Postgres ({detail}). Check the host and port in "
                "PACKFORM_PG_DSN. If Hermes runs in a container, 127.0.0.1 points at that "
                "container rather than the host - try the Docker bridge address 172.17.0.1, "
                "or the Postgres container name, or whatever host the n8n Postgres credential uses."
            )
        else:
            _diagnostics["postgres"] = (
                f"Connected to Postgres but the query failed ({detail}). Confirm "
                f"PACKFORM_TOKEN_TABLE (currently '{PG_TOKEN_TABLE}') and "
                f"PACKFORM_TOKEN_COLUMN (currently '{PG_TOKEN_COLUMN}') match the real schema."
            )
        return None

    if row and row[0]:
        _diagnostics["postgres"] = f"Read a token from {PG_TOKEN_TABLE}.{PG_TOKEN_COLUMN}."
        return str(row[0]).strip()

    _diagnostics["postgres"] = (
        f"Connected to Postgres, but {PG_TOKEN_TABLE} returned no rows. The n8n Access Token "
        "Save workflow may not have run yet, or the table name may be wrong."
    )
    return None


async def _token_from_direct_auth() -> Optional[str]:
    """Authenticate against Packform with stored credentials.

    Returns None when credentials are absent or the exchange fails.
    """
    if not (PACKFORM_USERNAME and PACKFORM_PASSWORD):
        _diagnostics["direct"] = "PACKFORM_USERNAME / PACKFORM_PASSWORD are not set, so direct auth is disabled."
        return None
    payload = {"username": PACKFORM_USERNAME, "password": PACKFORM_PASSWORD}
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.post(f"{API_BASE_URL}/api/auth/authenticate", json=payload)
            response.raise_for_status()
            body = response.json()
    except Exception as exc:
        _diagnostics["direct"] = (
            f"Direct authentication failed ({type(exc).__name__}). Packform issues Auth0 tokens, "
            "which often need more than a username and password, so prefer the Postgres source."
        )
        return None

    for key in ("access_token", "accessToken", "token", "id_token", "idToken"):
        value = body.get(key) if isinstance(body, dict) else None
        if value:
            _diagnostics["direct"] = "Obtained a token by authenticating directly."
            return str(value)

    _diagnostics["direct"] = (
        "Packform accepted the credentials but the response carried no recognisable token field. "
        "Use the Postgres source instead."
    )
    return None


async def _get_token(force_refresh: bool = False) -> str:
    """Return a usable bearer token, caching it for TOKEN_TTL_SECONDS."""
    now = time.time()
    cached = _token_cache.get("token")
    if cached and not force_refresh and (now - float(_token_cache["fetched_at"])) < TOKEN_TTL_SECONDS:
        return str(cached)

    _diagnostics.clear()
    token: Optional[str] = None
    source: Optional[str] = None

    if AUTH_MODE in ("postgres", "auto"):
        token = _token_from_postgres()
        source = "postgres" if token else None

    if token is None and AUTH_MODE in ("direct", "auto"):
        token = await _token_from_direct_auth()
        source = "direct" if token else None

    if token is None:
        raise PackformError(
            "No Packform token available. Either set PACKFORM_PG_DSN so the server can read "
            "the token the n8n Access Token Save workflow stores, or set PACKFORM_USERNAME "
            "and PACKFORM_PASSWORD for direct authentication. Run packform_health for detail."
        )

    _token_cache.update({"token": token, "fetched_at": now, "source": source})
    return token


# --------------------------------------------------------------------------
# HTTP plumbing
# --------------------------------------------------------------------------


async def _request(path: str, params: Optional[Dict[str, Any]] = None, _retry: bool = True) -> Any:
    """GET a Packform endpoint and return the decoded JSON body.

    Retries once with a freshly fetched token on a 401, which covers the case
    where the cached token expired between calls.
    """
    token = await _get_token()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    url = f"{API_BASE_URL}{path}"

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(url, headers=headers, params=params)

    if response.status_code == 401 and _retry:
        await _get_token(force_refresh=True)
        return await _request(path, params=params, _retry=False)

    response.raise_for_status()
    if not response.content:
        return None
    try:
        return response.json()
    except ValueError as exc:  # pragma: no cover - defensive
        raise PackformError(f"Packform returned a non-JSON body for {path}") from exc


def _handle_error(exc: Exception, context: str) -> str:
    """Format an exception as an actionable message for the agent."""
    if isinstance(exc, PackformError):
        return f"Error: {exc}"
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code == 401:
            return (
                f"Error: Packform rejected the token while {context}. The stored token may be "
                "stale. Run packform_health to check which auth source is in use."
            )
        if code == 403:
            return f"Error: This Packform account is not permitted to {context}."
        if code == 404:
            return (
                f"Error: Packform has no record for that identifier while {context}. "
                "If you used a short order number, resolve it first with packform_resolve_order."
            )
        if code == 429:
            return f"Error: Packform rate limited the request while {context}. Wait and retry."
        return f"Error: Packform returned HTTP {code} while {context}."
    if isinstance(exc, httpx.TimeoutException):
        return f"Error: Packform timed out while {context}. Retry; if it persists the API is slow or unreachable."
    if isinstance(exc, httpx.RequestError):
        return f"Error: Could not reach Packform while {context} ({type(exc).__name__})."
    return f"Error: Unexpected {type(exc).__name__} while {context}."


def _as_list(payload: Any) -> List[Dict[str, Any]]:
    """Coerce a Packform response into a list of records.

    Packform is inconsistent: some endpoints return a bare array, others wrap it
    in a key such as ``data``, ``items`` or ``results``.
    """
    if payload is None:
        return []
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "results", "content", "records"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []


def _render(payload: Any, fmt: ResponseFormat, title: str) -> str:
    """Render a payload in the requested format."""
    if fmt == ResponseFormat.JSON:
        return json.dumps(payload, indent=2, default=str)
    return f"# {title}\n\n```json\n{json.dumps(payload, indent=2, default=str)}\n```"


# --------------------------------------------------------------------------
# Margin arithmetic (arithmetic only - no thresholds, no verdicts)
# --------------------------------------------------------------------------


def _to_float(value: Any) -> Optional[float]:
    """Best-effort numeric coercion for figures that arrive as strings."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _first_number(record: Dict[str, Any], keys: List[str]) -> Optional[float]:
    """Return the first coercible number found under any of ``keys``."""
    for key in keys:
        number = _to_float(record.get(key))
        if number is not None:
            return number
    return None


SELL_KEYS = ["sellPrice", "sell_price", "price", "unitPrice", "unit_price", "customerPrice"]
COST_KEYS = ["buyPrice", "buy_price", "cost", "unitCost", "unit_cost", "supplierPrice"]
QTY_KEYS = ["quantity", "qty", "orderQuantity", "order_quantity"]


def _margin_for(record: Dict[str, Any]) -> Dict[str, Any]:
    """Compute gross margin for one record, when both figures are present.

    This is arithmetic, not a rule. No threshold is applied and no verdict is
    returned - the caller holds the thresholds.
    """
    sell = _first_number(record, SELL_KEYS)
    cost = _first_number(record, COST_KEYS)
    qty = _first_number(record, QTY_KEYS)

    result: Dict[str, Any] = {"sell": sell, "cost": cost, "quantity": qty}
    if sell is None or cost is None or sell == 0:
        result["margin_pct"] = None
        result["margin_amount"] = None
        result["note"] = "Insufficient data: both a sell and a cost figure are required."
        return result

    margin_amount = sell - cost
    result["margin_amount"] = round(margin_amount, 4)
    result["margin_pct"] = round((margin_amount / sell) * 100, 2)
    if qty is not None:
        result["extended_sell"] = round(sell * qty, 4)
        result["extended_margin_amount"] = round(margin_amount * qty, 4)
    return result


# --------------------------------------------------------------------------
# Input models
# --------------------------------------------------------------------------


class _Base(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, validate_assignment=True, extra="forbid")


class HealthInput(_Base):
    """Input model for the health check."""

    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )


class OrderInput(_Base):
    """Input model for any tool keyed on a single order."""

    order_id: str = Field(
        ...,
        description=(
            "Full Packform order UUID, e.g. 'a28cb156-656d-429b-9c90-190facd550c0'. "
            "If you only have a short number such as '#d550c0', call packform_resolve_order first."
        ),
        min_length=6,
        max_length=64,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )

    @field_validator("order_id")
    @classmethod
    def strip_hash(cls, value: str) -> str:
        return value.lstrip("#").strip()


class ResolveOrderInput(_Base):
    """Input model for resolving a short order number to a full UUID."""

    short_id: str = Field(
        ...,
        description=(
            "Short order number as it appears in Telegram reports and Packform emails, "
            "with or without the leading hash, e.g. '#d550c0' or 'd550c0'. This is the last "
            "six characters of the order UUID."
        ),
        min_length=4,
        max_length=12,
    )
    candidates: List[str] = Field(
        ...,
        description=(
            "Candidate full order UUIDs to match against, for example the order IDs already "
            "known from a previous tool call or from the Airtable order records."
        ),
        min_length=1,
        max_length=500,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )

    @field_validator("short_id")
    @classmethod
    def normalise(cls, value: str) -> str:
        return value.lstrip("#").strip().lower()


class ChatInput(_Base):
    """Input model for reading an order-scoped chat thread."""

    order_id: str = Field(..., description="Full Packform order UUID.", min_length=6, max_length=64)
    chat_id: str = Field(
        ...,
        description=(
            "Chat identifier. In Packform notification links this is the 'orderSelectedChatId' "
            "query parameter. Packform wraps the value in literal double quotes - pass it "
            "without them, the server handles encoding."
        ),
        min_length=4,
        max_length=128,
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )

    @field_validator("order_id", "chat_id")
    @classmethod
    def strip_quotes(cls, value: str) -> str:
        return value.strip().strip('"').lstrip("#").strip()


class DirectChatInput(_Base):
    """Input model for reading a direct (non order-scoped) chat thread."""

    account_id: str = Field(..., description="Packform account UUID for the counterparty.", min_length=6, max_length=64)
    chat_id: str = Field(..., description="Chat identifier for the direct thread.", min_length=4, max_length=128)
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )

    @field_validator("account_id", "chat_id")
    @classmethod
    def strip_quotes(cls, value: str) -> str:
        return value.strip().strip('"').strip()


class CategoriesInput(_Base):
    """Input model for the product category listing."""

    search: Optional[str] = Field(
        default=None,
        description="Case-insensitive substring to filter category names by, e.g. 'corrugated'.",
        max_length=200,
    )
    limit: int = Field(default=50, description="Maximum categories to return.", ge=1, le=500)
    offset: int = Field(default=0, description="Number of categories to skip, for pagination.", ge=0)
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )


class PastQuotesInput(_Base):
    """Input model for a product's historical quotes."""

    product_id: str = Field(..., description="Packform product (ancillary item) identifier.", min_length=1, max_length=64)
    months_to_include: int = Field(
        default=DEFAULT_MONTHS_TO_INCLUDE,
        description=(
            "How many months of history to include. Packform's own UI uses 3. Use 6 when "
            "checking against the six-month re-quote cycle."
        ),
        ge=1,
        le=36,
    )
    with_po: Optional[bool] = Field(
        default=None,
        description=(
            "True returns history that resulted in a purchase order (ordered history). "
            "False returns quoted-but-not-ordered history. Omit to fetch both and label them."
        ),
    )
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )


class MarginInput(_Base):
    """Input model for the margin arithmetic helper."""

    order_id: str = Field(..., description="Full Packform order UUID.", min_length=6, max_length=64)
    response_format: ResponseFormat = Field(
        default=ResponseFormat.MARKDOWN, description="Output format: 'markdown' or 'json'."
    )

    @field_validator("order_id")
    @classmethod
    def strip_hash(cls, value: str) -> str:
        return value.lstrip("#").strip()


def _read_only(title: str) -> ToolAnnotations:
    """Annotations shared by every tool in this server: all are read-only."""
    return ToolAnnotations(
        title=title,
        read_only_hint=True,
        destructive_hint=False,
        idempotent_hint=True,
        open_world_hint=True,
    )


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------


@mcp.tool(name="packform_health", annotations=_read_only("Packform Connection Health"))
async def packform_health(params: HealthInput) -> str:
    """Check that Packform is reachable and that a bearer token can be obtained.

    Run this first when any other tool reports an authentication problem. It
    reports which auth source supplied the token without ever revealing the
    token itself.

    Args:
        params (HealthInput): Validated input containing:
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Report with the following schema:
        {
            "base_url": str,          # Packform base URL in use
            "auth_mode": str,         # configured mode: postgres | direct | auto
            "token_source": str,      # which source supplied the token, or null
            "token_present": bool,    # whether a token was obtained
            "api_reachable": bool,    # whether an authenticated call succeeded
            "writes_enabled": bool,   # always false in this build
            "detail": str             # human-readable summary
        }

    Examples:
        - Use when: another tool returned an authentication error.
        - Use when: confirming the server is configured after installation.
        - Don't use when: you simply want order data (call the specific tool).
    """
    report: Dict[str, Any] = {
        "base_url": API_BASE_URL,
        "auth_mode": AUTH_MODE,
        "token_source": None,
        "token_present": False,
        "api_reachable": False,
        "writes_enabled": ALLOW_WRITES,
        "detail": "",
    }
    try:
        await _get_token()
        report["token_present"] = True
        report["token_source"] = _token_cache.get("source")
    except PackformError as exc:
        report["detail"] = str(exc)
        report["auth_diagnostics"] = dict(_diagnostics)
        return _render(report, params.response_format, "Packform Health")

    report["auth_diagnostics"] = dict(_diagnostics)

    try:
        await _request("/api/distributor/product-categories")
        report["api_reachable"] = True
        report["detail"] = f"Token obtained from {report['token_source']}; Packform responded successfully."
    except Exception as exc:
        report["detail"] = _handle_error(exc, "checking API reachability")

    return _render(report, params.response_format, "Packform Health")


@mcp.tool(name="packform_resolve_order", annotations=_read_only("Resolve Short Order Number"))
async def packform_resolve_order(params: ResolveOrderInput) -> str:
    """Match a short order number such as '#d550c0' to a full order UUID.

    Packform's emails and the Telegram reports refer to orders by the last six
    characters of the UUID. Every other tool needs the full UUID. This tool does
    that matching locally against candidate UUIDs you supply - it makes no API
    call, so it cannot discover an order you do not already know about.

    Args:
        params (ResolveOrderInput): Validated input containing:
            - short_id (str): Short order number, with or without '#'.
            - candidates (List[str]): Full order UUIDs to match against.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "short_id": str,           # normalised short id searched for
            "match_count": int,        # number of candidates that matched
            "order_id": str | null,    # the single match, when exactly one
            "matches": [str],          # every matching UUID
            "note": str                # guidance when zero or several matched
        }

    Examples:
        - Use when: a report or a person refers to "order #d550c0".
        - Use when: an email subject carries a short id and you need the record.
        - Don't use when: you already hold the full UUID.

    Error Handling:
        - Zero matches returns match_count 0 with a note; it is not an error.
        - Several matches returns them all so the caller can disambiguate.
    """
    short = params.short_id
    matches = [c.strip() for c in params.candidates if c and c.strip().lower().endswith(short)]

    result: Dict[str, Any] = {
        "short_id": short,
        "match_count": len(matches),
        "order_id": matches[0] if len(matches) == 1 else None,
        "matches": matches,
        "note": "",
    }
    if not matches:
        result["note"] = (
            f"No candidate ends with '{short}'. Widen the candidate list - for example by "
            "listing recent orders from Airtable - and try again."
        )
    elif len(matches) > 1:
        result["note"] = "Several candidates share that suffix. Disambiguate by customer or date before acting."

    return _render(result, params.response_format, f"Resolve order #{short}")


@mcp.tool(name="packform_get_order", annotations=_read_only("Get Packform Order"))
async def packform_get_order(params: OrderInput) -> str:
    """Fetch the full record for one order.

    This is the order header: customer, stage, dates and totals as Packform holds
    them. For the line items use packform_get_order_items, and for supplier-side
    state use packform_get_supplier_orders.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: The order record as Packform returns it, rendered in the requested
        format. Field names are Packform's own and vary by order stage.

    Examples:
        - Use when: "what's the status of order a28cb156-...".
        - Use when: you need the customer or stage before deciding anything.
        - Don't use when: you need line items or supplier quotes.

    Error Handling:
        - Returns a 404 message suggesting packform_resolve_order when the UUID
          is unknown, which usually means a short id was passed by mistake.
    """
    try:
        data = await _request(f"/api/distributor/orders/{params.order_id}")
        return _render(data, params.response_format, f"Order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching order {params.order_id}")


@mcp.tool(name="packform_get_order_items", annotations=_read_only("Get Order Line Items"))
async def packform_get_order_items(params: OrderInput) -> str:
    """List the line items on an order.

    Each item is a product with its quantity and, where Packform holds them, the
    sell and cost figures. Use packform_get_order_margin when you want those
    figures turned into margin percentages.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,     # the order queried
            "count": int,        # number of line items
            "items": [ {...} ]   # Packform line item records, fields vary
        }

    Examples:
        - Use when: "what did they actually order".
        - Use when: you need product ids before calling packform_get_past_quotes.
        - Don't use when: you want only the outstanding ones (use
          packform_get_outstanding_items).
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/items")
        items = _as_list(payload)
        result = {"order_id": params.order_id, "count": len(items), "items": items}
        return _render(result, params.response_format, f"Items on order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching items for order {params.order_id}")


@mcp.tool(name="packform_get_outstanding_items", annotations=_read_only("Get Outstanding Items"))
async def packform_get_outstanding_items(params: OrderInput) -> str:
    """List items still awaiting a supplier response, with their live status.

    This carries the Submitted and Pending state per item per supplier, which is
    the state the sourcing round logic reads. It is the tool to use when asking
    whether a round can be decided or is still waiting.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "count": int,
            "items": [ {...} ]   # per item per supplier status records
        }

    Examples:
        - Use when: "who hasn't come back on this order yet".
        - Use when: deciding whether a sourcing round is still open.
        - Don't use when: you want every item regardless of state.
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/outstanding-items")
        items = _as_list(payload)
        result = {"order_id": params.order_id, "count": len(items), "items": items}
        return _render(result, params.response_format, f"Outstanding items on order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching outstanding items for order {params.order_id}")


@mcp.tool(name="packform_get_quote_requests", annotations=_read_only("Get Quote Requests"))
async def packform_get_quote_requests(params: OrderInput) -> str:
    """List the quote requests issued to suppliers for an order.

    Shows which suppliers were asked, for what, and where each request stands.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "count": int,
            "quote_requests": [ {...} ]
        }

    Examples:
        - Use when: "which suppliers did we ask".
        - Use when: checking whether a supplier was approached twice.
        - Don't use when: you want the resulting purchase orders (use
          packform_get_supplier_orders).
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/quote-requests")
        rows = _as_list(payload)
        result = {"order_id": params.order_id, "count": len(rows), "quote_requests": rows}
        return _render(result, params.response_format, f"Quote requests on order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching quote requests for order {params.order_id}")


@mcp.tool(name="packform_get_supplier_orders", annotations=_read_only("Get Supplier Orders"))
async def packform_get_supplier_orders(params: OrderInput) -> str:
    """List the supplier-side orders (purchase orders) for an order.

    Carries the accepted supplier, the PO number, and the delivery status.
    Observed delivery statuses are 'pending_confirmation', 'processing' and
    'done'.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "count": int,
            "supplier_orders": [ {...} ]   # includes acceptedSupplierId, short_id, deliveryStatus
        }

    Examples:
        - Use when: "has the PO been confirmed".
        - Use when: you need the supplier name attached to an order.
        - Don't use when: the PO has not been raised yet (use
          packform_get_quote_requests).
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/suppliers-orders")
        rows = _as_list(payload)
        result = {"order_id": params.order_id, "count": len(rows), "supplier_orders": rows}
        return _render(result, params.response_format, f"Supplier orders on {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching supplier orders for {params.order_id}")


@mcp.tool(name="packform_get_deliveries", annotations=_read_only("Get Order Deliveries"))
async def packform_get_deliveries(params: OrderInput) -> str:
    """List deliveries recorded against an order.

    This is the fulfilment side: what has shipped, when, and against which
    supplier order.

    Args:
        params (OrderInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "count": int,
            "deliveries": [ {...} ]
        }

    Examples:
        - Use when: "has any of this shipped yet".
        - Use when: checking a delivery against a promised date.
        - Don't use when: the order has not reached PO confirmation.
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/deliveries")
        rows = _as_list(payload)
        result = {"order_id": params.order_id, "count": len(rows), "deliveries": rows}
        return _render(result, params.response_format, f"Deliveries on order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"fetching deliveries for order {params.order_id}")


@mcp.tool(name="packform_get_order_chat", annotations=_read_only("Read Order Message Thread"))
async def packform_get_order_chat(params: ChatInput) -> str:
    """Read a message thread attached to an order.

    Returns the messages themselves rather than a notification that a message
    exists, so the content can be reasoned about directly.

    Args:
        params (ChatInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - chat_id (str): Chat identifier from the notification link's
              'orderSelectedChatId' parameter, without surrounding quotes.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "chat_id": str,
            "count": int,
            "messages": [ {...} ]
        }

    Examples:
        - Use when: a customer or supplier has written on an order.
        - Use when: you need the text before drafting a reply for a human.
        - Don't use when: the thread is not order-scoped (use
          packform_get_direct_chat).

    Error Handling:
        - Packform wraps chat ids in literal double quotes inside URLs; the
          server strips and encodes them, so pass the bare value.
    """
    try:
        payload = await _request(f"/api/distributor/chats/order/{params.order_id}/{params.chat_id}")
        rows = _as_list(payload)
        result = {
            "order_id": params.order_id,
            "chat_id": params.chat_id,
            "count": len(rows),
            "messages": rows,
        }
        return _render(result, params.response_format, f"Messages on order {params.order_id}")
    except Exception as exc:
        return _handle_error(exc, f"reading the chat on order {params.order_id}")


@mcp.tool(name="packform_get_direct_chat", annotations=_read_only("Read Direct Message Thread"))
async def packform_get_direct_chat(params: DirectChatInput) -> str:
    """Read a direct message thread that is not attached to an order.

    Args:
        params (DirectChatInput): Validated input containing:
            - account_id (str): Packform account UUID for the counterparty.
            - chat_id (str): Chat identifier for the thread.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "account_id": str,
            "chat_id": str,
            "count": int,
            "messages": [ {...} ]
        }

    Examples:
        - Use when: someone messaged outside the context of an order.
        - Don't use when: the thread belongs to an order (use
          packform_get_order_chat).
    """
    try:
        payload = await _request(f"/api/distributor/chats/direct/{params.account_id}/{params.chat_id}")
        rows = _as_list(payload)
        result = {
            "account_id": params.account_id,
            "chat_id": params.chat_id,
            "count": len(rows),
            "messages": rows,
        }
        return _render(result, params.response_format, "Direct messages")
    except Exception as exc:
        return _handle_error(exc, "reading the direct chat")


@mcp.tool(name="packform_get_product_categories", annotations=_read_only("List Product Categories"))
async def packform_get_product_categories(params: CategoriesInput) -> str:
    """List Packform's product categories, optionally filtered and paginated.

    The full catalogue runs to thousands of categories, so always pass a search
    term or a modest limit rather than pulling everything into context.

    Args:
        params (CategoriesInput): Validated input containing:
            - search (Optional[str]): Case-insensitive substring filter.
            - limit (int): Maximum to return, 1-500 (default 50).
            - offset (int): Number to skip, for pagination (default 0).
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "total": int,          # total matching the filter
            "count": int,          # number returned in this response
            "offset": int,
            "has_more": bool,
            "next_offset": int | null,
            "categories": [ {...} ]
        }

    Examples:
        - Use when: resolving a product description to a catalogue category.
        - Don't use when: you already hold a product id.
    """
    try:
        payload = await _request("/api/distributor/product-categories")
        rows = _as_list(payload)

        if params.search:
            needle = params.search.lower()
            rows = [
                row
                for row in rows
                if needle in str(row.get("name", "")).lower() or needle in str(row.get("code", "")).lower()
            ]

        total = len(rows)
        window = rows[params.offset : params.offset + params.limit]
        has_more = total > params.offset + len(window)
        result = {
            "total": total,
            "count": len(window),
            "offset": params.offset,
            "has_more": has_more,
            "next_offset": params.offset + len(window) if has_more else None,
            "categories": window,
        }
        return _render(result, params.response_format, "Product categories")
    except Exception as exc:
        return _handle_error(exc, "listing product categories")


@mcp.tool(name="packform_get_past_quotes", annotations=_read_only("Get Product Quote History"))
async def packform_get_past_quotes(params: PastQuotesInput) -> str:
    """Fetch historical quotes for a product, ordered and unordered.

    Packform separates history that resulted in a purchase order from history
    that did not. Omitting with_po fetches both and labels them, which is
    usually what you want when pricing something that has been quoted before.

    Args:
        params (PastQuotesInput): Validated input containing:
            - product_id (str): Packform product identifier.
            - months_to_include (int): Months of history, 1-36 (default 3).
            - with_po (Optional[bool]): True for ordered history, False for
              quoted-only, omit for both.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "product_id": str,
            "months_to_include": int,
            "ordered": [ {...} ] | null,      # history with a purchase order
            "quoted_only": [ {...} ] | null,  # history without one
            "errors": { "ordered": str, "quoted_only": str }   # present only on partial failure
        }

    Examples:
        - Use when: pricing an item that may have been quoted before.
        - Use when: checking a cost against the six-month re-quote cycle
          (pass months_to_include=6).
        - Don't use when: the item is genuinely new; there will be no history.

    Error Handling:
        - If one of the two fetches fails the other is still returned, with the
          failure recorded under "errors".
    """
    path = f"/api/distributor/product/ancillary/{params.product_id}/past-quotes"
    result: Dict[str, Any] = {
        "product_id": params.product_id,
        "months_to_include": params.months_to_include,
        "ordered": None,
        "quoted_only": None,
    }
    errors: Dict[str, str] = {}

    wanted: List[bool] = [params.with_po] if params.with_po is not None else [True, False]
    for flag in wanted:
        key = "ordered" if flag else "quoted_only"
        try:
            payload = await _request(
                path,
                params={"monthsToInclude": params.months_to_include, "withPO": str(flag).lower()},
            )
            result[key] = _as_list(payload)
        except Exception as exc:
            errors[key] = _handle_error(exc, f"fetching {key} history for product {params.product_id}")

    if errors:
        result["errors"] = errors
    if result["ordered"] is None and result["quoted_only"] is None:
        return _handle_error(
            PackformError("both history fetches failed; see the errors field"),
            f"fetching quote history for product {params.product_id}",
        )

    return _render(result, params.response_format, f"Quote history for {params.product_id}")


@mcp.tool(name="packform_get_order_margin", annotations=_read_only("Compute Order Margin"))
async def packform_get_order_margin(params: MarginInput) -> str:
    """Compute gross margin per line and for the order, from Packform's figures.

    This is arithmetic only. It applies no thresholds and returns no verdict -
    the margin bands and the hard stop live in the n8n workflows, and the caller
    holds those numbers as context. The point of this tool is that the agent
    never has to do the arithmetic itself.

    Order margin is dollar-weighted: the sum of line margin dollars divided by
    the sum of line sell dollars, which is how the book margin is calculated.
    Lines missing either a sell or a cost figure are excluded and counted.

    Args:
        params (MarginInput): Validated input containing:
            - order_id (str): Full Packform order UUID.
            - response_format (ResponseFormat): 'markdown' or 'json'.

    Returns:
        str: Result with the following schema:
        {
            "order_id": str,
            "line_count": int,              # total lines on the order
            "priced_line_count": int,       # lines with both figures present
            "skipped_line_count": int,      # lines excluded for missing figures
            "order_margin_pct": float|null, # dollar-weighted, 2dp
            "total_sell": float|null,
            "total_margin_amount": float|null,
            "lines": [
                {
                    "sell": float|null,
                    "cost": float|null,
                    "quantity": float|null,
                    "margin_pct": float|null,
                    "margin_amount": float|null,
                    "extended_sell": float,        # when quantity is present
                    "extended_margin_amount": float,
                    "note": str                    # when figures were missing
                }
            ]
        }

    Examples:
        - Use when: "what's the margin on this order".
        - Use when: you need a margin figure before describing it against the
          21% flag or the 5% floor.
        - Don't use when: you want a decision - this tool does not make one.

    Error Handling:
        - Returns order_margin_pct null when no line carries both figures, with
          skipped_line_count explaining why.
    """
    try:
        payload = await _request(f"/api/distributor/orders/{params.order_id}/items")
        items = _as_list(payload)
    except Exception as exc:
        return _handle_error(exc, f"fetching items to compute margin for {params.order_id}")

    lines = [_margin_for(item) for item in items]
    priced = [line for line in lines if line.get("margin_pct") is not None]

    total_sell = 0.0
    total_margin = 0.0
    for line in priced:
        qty = line.get("quantity") or 1.0
        total_sell += float(line["sell"]) * float(qty)
        total_margin += float(line["margin_amount"]) * float(qty)

    result = {
        "order_id": params.order_id,
        "line_count": len(lines),
        "priced_line_count": len(priced),
        "skipped_line_count": len(lines) - len(priced),
        "order_margin_pct": round((total_margin / total_sell) * 100, 2) if total_sell else None,
        "total_sell": round(total_sell, 4) if total_sell else None,
        "total_margin_amount": round(total_margin, 4) if total_sell else None,
        "lines": lines,
    }
    return _render(result, params.response_format, f"Margin on order {params.order_id}")


def main() -> None:
    """Console-script entry point. Serves MCP over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
