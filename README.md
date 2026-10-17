# Packform MCP

Read-only MCP server interacting the Packform distributor API to Hermes Agent.

Twelve tools covering the order lifecycle: orders, line items, outstanding
items, quote requests, supplier orders, deliveries, message threads, the
product catalogue, quote history, and margin arithmetic. Plus a resolver that
turns the short order numbers used in emails and Telegram reports into the full
UUIDs every other tool needs.

**The server performs no writes.** Nothing it does can change anything in
Packform.

## What it deliberately does not do

It contains no business rules. Margin bands, the hard stop, round logic and
escalation thresholds live in the n8n workflows. `packform_get_order_margin`
returns a margin *percentage* — arithmetic, not a verdict — so the agent never
has to do the maths itself but is never the thing deciding either.

## Install

Python 3.10 or newer.

```bash
git clone <this repo> packform-mcp
cd packform-mcp
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Configure

Secrets live outside the repository.

```bash
mkdir -p ~/.config/packform-mcp && chmod 700 ~/.config/packform-mcp
cp .env.example ~/.config/packform-mcp/env
chmod 600 ~/.config/packform-mcp/env
```

Then edit that file. The only decision is where the bearer token comes from.

**Option 1 — reuse the token n8n already refreshes.** The existing "Access
Token Save" workflow authenticates every ten hours and writes the token to
Postgres. Point `PACKFORM_PG_DSN` at that database and the server reads the
same token. One refresh path, one copy of the credential.

If the column names differ from the defaults, set `PACKFORM_TOKEN_TABLE` and
`PACKFORM_TOKEN_COLUMN` to match. To see them:

```sql
\d access_token
```

**Option 2 — authenticate directly.** Set `PACKFORM_USERNAME` and
`PACKFORM_PASSWORD`. Independent of n8n, but a second copy of the credentials.

`PACKFORM_AUTH_MODE=auto` (the default) tries Postgres first and falls back to
direct auth, so the server keeps working if the n8n side is being changed.

## Register with Hermes Agent

Add to `mcp_servers` in `~/.hermes/config.yaml`, using absolute paths:

```yaml
mcp_servers:
  packform:
    command: /opt/data/mcp-installs/packform/.venv/bin/python
    args:
      - /opt/data/mcp-installs/packform/server.py
    env:
      PACKFORM_MCP_ENV: /root/.config/packform-mcp/env
    timeout: 120
    connect_timeout: 30
```

Then `/reload-mcp` inside Hermes.

Alternatively use ADD SERVER in the Hermes dashboard and point it at this
repository; the bootstrap is the two commands under Install.

## Verify

Run `packform_health` first. It reports the base URL, which auth source
supplied the token, and whether an authenticated call succeeded — without ever
printing the token.

A good first real question, once it is connected:

> What's the status of order `<full uuid>`, who has quoted, and what's the
> margin?

That exercises the order, supplier-order and margin tools in one pass.

## Tools

| Tool | Purpose |
| --- | --- |
| `packform_health` | Connectivity and auth check |
| `packform_resolve_order` | Short order number (`#d550c0`) → full UUID |
| `packform_get_order` | Order header |
| `packform_get_order_items` | Line items |
| `packform_get_outstanding_items` | Items still awaiting supplier response |
| `packform_get_quote_requests` | Quote requests issued to suppliers |
| `packform_get_supplier_orders` | Supplier orders, PO numbers, delivery status |
| `packform_get_deliveries` | Deliveries recorded |
| `packform_get_order_chat` | Order-scoped message thread |
| `packform_get_direct_chat` | Direct message thread |
| `packform_get_product_categories` | Catalogue, filterable and paginated |
| `packform_get_past_quotes` | Ordered and quoted-only history for a product |
| `packform_get_order_margin` | Per-line and dollar-weighted order margin |

## Notes on Packform's API

The short order number in emails and reports is the **last six characters** of
the order UUID. `packform_resolve_order` matches locally against candidates you
supply; it makes no API call and so cannot find an order you do not already
know about.

Chat identifiers arrive from notification links wrapped in literal double
quotes. Pass the bare value — the server strips and encodes it.

`deliveryStatus` values observed so far: `pending_confirmation`, `processing`,
`done`.

Quote history is split by whether a purchase order resulted. Omitting `with_po`
fetches both and labels them.

Responses are inconsistently shaped — some endpoints return a bare array,
others wrap it in `data`, `items` or `results`. The server normalises this.

## Licence

MIT
