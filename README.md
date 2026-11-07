# hacker-watch

Live tracer for the NEAR wallet `efd224256518a7cacd209867963276efed302a8e222c013c43f4b1bd369ae5af`, funded with 12.41M NEAR by three Binance hot wallets on 2026-10-29.

`watch.py` follows every outgoing NEAR transfer from that wallet, keeps walking the fresh accounts it funds, and works out where value leaves NEAR:

| Exit | How it is resolved |
|---|---|
| 1Click / NEAR Intents swap | Each new receiver is looked up on the public 1Click status API (`https://1click.chaindefuser.com/v0/status?depositAddress=<account>`). A match is a per-quote deposit address and gives the destination asset, recipient address, amounts and destination-chain tx. |
| Omni Bridge | `ft_transfer_call` from `wrap.near` to `omni.bridge.near`; the `InitTransferEvent` log gives the destination chain and recipient. |
| Intents deposit | `mt_mint` events from `intents.near`. |
| Swap service → Binance | Payments into the shared deposit address `ba3ef477…` of an unnamed swap service. The service forwards each sweep to its own Binance deposit address `6ac2e1a1…` within a minute, so this is a CEX exit at Binance. |
| Exolix | Payments into Exolix deposit addresses. Exolix reuses deposit addresses across customers; each one forwards to Exolix's NEAR hot wallet `e5894573…` (proven with a test order on exolix.com). |
| Exchange deposit addresses | Any account that sends 90% or more of its NEAR to one known exchange hot wallet (`CEX_HOT`: Binance, the exchange behind `30d6205b…`, Exolix) is that exchange's deposit address, so a cluster payment into it is an exit. Such accounts leave the cluster instead of being walked. |

Busy accounts (over 1,000 txs) and known services are endpoints and are not walked further. Fresh accounts are followed up to 8 hops from the root. Exits dated before the incident (2026-10-29 20:15 UTC) are dropped from the outputs.

`30d6205b…` is not Binance: it has its own wallet family and no flow with Binance's hubs (reported as OKX). `ccb91e1d…` is kept as "Binance (unconfirmed)": it was labelled from its sweep pattern only.

It also checks the exit recipients off NEAR: BTC addresses on mempool.space, and EVM addresses (Ethereum, Base, Arbitrum, BNB Chain) for native balance, Omni-bridged NEAR token balance and outgoing tx count.

Only the Python 3 standard library is used (tested on 3.9).

## Setup

```bash
cp .env.example .env    # then put your Pikespeak key (and ideally a FastNEAR key) in .env
python3 watch.py --once --no-notify   # first run walks the whole history, a few minutes
```

Keys are personal: get your own at pikespeak.ai and fastnear.com rather than reusing someone else's.

## Run it live

```bash
nohup python3 watch.py --interval 120 >> watch.log 2>&1 & echo $! > watch.pid
tail -f watch.log
kill $(cat watch.pid)   # stop
```

Every cycle (default 120 s) it checks balances of every traced account, rescans any that changed, refreshes pending 1Click swaps, and rewrites the outputs. On macOS it also posts a notification when the root wallet moves, when value leaves NEAR, or when a new hop receives 1,000 NEAR or more. Everything is logged to stdout either way.

Options: `--once` (walk until nothing new, then exit), `--interval N` (seconds between cycles), `--no-notify`. Set `WATCH_ROOT` in `.env` to trace a different wallet (start with a fresh `watch.db`).

## Outputs

| File | What it is |
|---|---|
| `watch.db` | SQLite state: accounts, transfers, exits, BTC and EVM balances. Delete it to start over. |
| `report.md` | Human-readable summary: totals, exit log, BTC recipients, wallets still holding NEAR, all accounts. |
| `edges.csv` | Every NEAR transfer seen, for graphing. |
| `export/*.json` | Row files (`accounts`, `exits`, `btc`, `evm`) behind the claude.ai dashboard, plus `venues.json` (CEX exits through services: route, deposits, NEAR in, NEAR still held, who can act) and `holds.json` (live balances of the service and exchange deposit addresses where exited NEAR is sitting). Venue exits appear in `exits.json` with `route` = the venue (`Swap service → Binance`, `Exolix`, …), `asset_out` = `NEAR` and `deposit_address` = where it was paid. |

## Dashboard

`dashboard/index.html` is the page of a claude.ai Dashboard artifact that reads the four `export/*.json` files as datasets. Artifact pages cannot call outside APIs, so the data is pushed: upload the JSON files as assets of the artifact and point each dataset at the new file. In the original setup a Claude scheduled task does that every 30 minutes.

## Adding to it

- New exit routes: add a classifier in `classify()` or a log parser in `parse_tx()`, and write a row into `exits`.
- New services to stop at: add them to `SERVICES` (or `BINANCE`).
- New chains for recipient balances: add an RPC and token address to `EVM`.

## Data sources

Pikespeak (NEAR transfers, tx counts), FastNEAR RPC and archival RPC (balances, tx logs), Nearblocks (contract calls of an account), 1Click status API (Intents swaps), mempool.space (BTC), publicnode RPCs (EVM).
