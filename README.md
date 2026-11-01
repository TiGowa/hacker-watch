# hacker-watch

Live tracer for the NEAR wallet `efd224256518a7cacd209867963276efed302a8e222c013c43f4b1bd369ae5af`, funded with 12.41M NEAR by three Binance hot wallets on 2026-10-29.

`watch.py` follows every outgoing NEAR transfer from that wallet, keeps walking the fresh accounts it funds, and works out where value leaves NEAR:

| Exit | How it is resolved |
|---|---|
| 1Click / NEAR Intents swap | Each new receiver is looked up on the public 1Click status API (`https://1click.chaindefuser.com/v0/status?depositAddress=<account>`). A match is a per-quote deposit address and gives the destination asset, recipient address, amounts and destination-chain tx. |
| Omni Bridge | `ft_transfer_call` from `wrap.near` to `omni.bridge.near`; the `InitTransferEvent` log gives the destination chain and recipient. |
| Intents deposit | `mt_mint` events from `intents.near`. |
| Binance | Transfers to the six known Binance NEAR hub wallets. |

Busy accounts (over 1,000 txs) and known services are endpoints and are not walked further. Fresh accounts are followed up to 8 hops from the root.

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
| `export/*.json` | Row files (`accounts`, `exits`, `btc`, `evm`) behind the claude.ai dashboard. |

## Dashboard

`dashboard/index.html` is the page of a claude.ai Dashboard artifact that reads the four `export/*.json` files as datasets. Artifact pages cannot call outside APIs, so the data is pushed: upload the JSON files as assets of the artifact and point each dataset at the new file. In the original setup a Claude scheduled task does that every 30 minutes.

## Adding to it

- New exit routes: add a classifier in `classify()` or a log parser in `parse_tx()`, and write a row into `exits`.
- New services to stop at: add them to `SERVICES` (or `BINANCE`).
- New chains for recipient balances: add an RPC and token address to `EVM`.

## Data sources

Pikespeak (NEAR transfers, tx counts), FastNEAR RPC and archival RPC (balances, tx logs), Nearblocks (contract calls of an account), 1Click status API (Intents swaps), mempool.space (BTC), publicnode RPCs (EVM).
