#!/usr/bin/env python3
"""Live watcher for the efd22425… NEAR wallet (the "binance hacker").

Follows every outgoing NEAR transfer from the root wallet, keeps walking the
fresh accounts it funds, and resolves where value leaves NEAR:

  - 1Click / NEAR Intents deposit addresses -> destination asset + recipient
  - Omni Bridge transfers (wrap.near -> omni.bridge.near) -> chain + recipient
  - Intents deposits (mt_mint into intents.near)          -> intents owner
  - Service / exchange deposit addresses                   -> CEX exit (swap
    service that banks at Binance, Exolix, any account forwarding to a known
    exchange hot wallet)
  - Busy service accounts                                  -> terminal label

State lives in watch.db; report.md and edges.csv are rewritten every cycle.
Alerts go to watch.log (stdout) and macOS notifications.

    python3 watch.py              # loop forever, 120 s between cycles
    python3 watch.py --once       # one cycle, then exit
"""
import argparse
import csv
import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))


def load_env(path=os.path.join(HERE, ".env")):
    """KEY=value lines from .env (kept out of git); real env vars win."""
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
ROOT = os.environ.get("WATCH_ROOT", "efd224256518a7cacd209867963276efed302a8e222c013c43f4b1bd369ae5af")
ROOT_FUNDED = 12_410_067.6  # three Binance hub sends on 2026-10-29

PIKE = "https://api.pikespeak.ai"
PIKE_KEY = os.environ.get("PIKESPEAK_KEY", "")
RPC = "https://rpc.mainnet.fastnear.com"
ARCHIVAL = "https://archival-rpc.mainnet.fastnear.com"
# optional, but keyless FastNEAR returns 429 at ~10 req/s
FASTNEAR_KEY = os.environ.get("FASTNEAR_API_KEY", "")
NEARBLOCKS = "https://api.nearblocks.io/v1"
ONECLICK = "https://1click.chaindefuser.com/v0/status?depositAddress="
MEMPOOL = "https://mempool.space/api/address/"
# EVM chains exits can land on: public RPC + Omni-bridged NEAR token there
EVM = {
    "eth": ("https://ethereum-rpc.publicnode.com", "0x85f17cf997934a597031b2e18a9ab6ebd4b9f6a4"),
    "base": ("https://base-rpc.publicnode.com", "0x02eea354d135d1a912967c2d2a6147deb01ef92e"),
    "arb": ("https://arbitrum-one-rpc.publicnode.com", "0x02eea354d135d1a912967c2d2a6147deb01ef92e"),
    "bnb": ("https://bsc-rpc.publicnode.com", "0x028e0686cb8458e84f0925a051c82227f958fdb1"),
}

# Exchange / service hot wallets. Their deposit addresses forward (sweep) into
# them, so an account that sends ~all its NEAR to one of these is a deposit
# address of that venue, and a cluster payment into it is a CEX exit.
CEX_HOT = {
    "5c33c6218d47e00ef229f60da78d0897e1ee9665312550b8afd5f9c7bc6957d2": "Binance",
    "451913e633c72fb951effe076d3bd2977e3f08083dcf6c87fa89992b0d437c5c": "Binance",
    "9c484fa5d2d069569ba063fc555c34e621ccd88fdbb0295fc79bad232621c5c1": "Binance",
    "d04204b6591fefb973a8ed60929f8211af89ee38020da9faf905acdc7a320063": "Binance",
    "7747991786f445efb658b69857eadc7a57b6b475beec26ed14da8bc35bb2b5b6": "Binance",
    # sweep pattern only, never linked to a named Binance wallet
    "ccb91e1db61e8d7e1d4ae3e043001140132959a86ee35a548b6563a46284a6ea": "Binance (unconfirmed)",
    # own wallet family, no flow with Binance hubs; reported as OKX
    "30d6205b89e02d2c34ade165ae9705f8d9e80d0e304061cb10b8e56c8e703f62": "Exchange 30d6205b (reported OKX)",
    # Exolix's site hands out reused deposit addresses that forward here (test order, Nov 6)
    "e589457354361489a89039b8be6737cbc2db4d13919b6ccf23889a60f3b0d8f3": "Exolix",
}
BINANCE = {a for a, v in CEX_HOT.items() if v == "Binance"}
# Deposit addresses of services that end at an exchange: a cluster payment into
# one is an exit. route, label, who can act.
SWAP_SERVICE = "Unnamed swap service (hot wallet e292f41c…, banks at Binance via 6ac2e1a1…)"
VENUE_DEPOSITS = {
    "ba3ef477392ff17736e8b99b10df03487272868aaf84bb5702e9ea7433b5cc0d": ("Swap service → Binance", "Swap service shared deposit address", SWAP_SERVICE),
    "e292f41cccbf8500ccc64e9a767333d73b0e7360abba6c9008f280f9b3995982": ("Swap service → Binance", "Swap service hot wallet", SWAP_SERVICE),
    "6ac2e1a161f02e3e8fed6cfa1b249d0f015c0f5d7968c7d8ef482d65995f507b": ("Swap service → Binance", "Swap service's Binance deposit address", "Binance"),
}
# Balances worth showing as "freezable": where exited NEAR is still sitting.
# Hot wallets (the swap service's, Exolix's) are left out: their balance is
# the business's float, not the cluster's money.
VENUE_HOLDS = {
    "ba3ef477392ff17736e8b99b10df03487272868aaf84bb5702e9ea7433b5cc0d": ("Swap service → Binance", "Swap service shared deposit address", SWAP_SERVICE),
    "6ac2e1a161f02e3e8fed6cfa1b249d0f015c0f5d7968c7d8ef482d65995f507b": ("Swap service → Binance", "Binance deposit address of the swap service (unswept since Oct 29)", "Binance"),
}
VENUE_CONTACTS = {"Swap service → Binance": "Binance, then the swap service", "Exolix": "Exolix"}
FORWARD_SHARE = 0.9  # share of an account's NEAR out going to one CEX hot wallet to call it a deposit address
INCIDENT_NS = int(datetime(2026, 10, 29, 20, 15, tzinfo=timezone.utc).timestamp()) * 10**9  # ignore earlier rows
SERVICES = {
    "wrap.near": "wNEAR wrapper",
    "omni.bridge.near": "Omni Bridge",
    "intents.near": "NEAR Intents",
    "bridge-mng.near": "Intents PoA bridge",
    "omft.near": "Intents PoA token factory",
    "v2_1.omni.hot.tg": "HOT Omni bridge",
    "aggregatedex.near": "DEX aggregator",
    "v2.ref-finance.near": "Rhea DEX",
    "dclv2.ref-labs.near": "Rhea DCL",
    "team.herewallet.near": "HERE Wallet sponsor",
    "free-name.herewallet.near": "HERE Wallet sponsor",
}
# NEP-141 tokens seen moving through bridges: label, decimals, counts as USD
TOKENS = {
    "wrap.near": ("wNEAR", 24, False),
    "usdt.tether-token.near": ("USDt", 6, True),
    "dac17f958d2ee523a2206206994597c13d831ec7.factory.bridge.near": ("USDT.e", 6, True),
    "17208628f84f5d6ad33f0da3bbbeb27ffcb398eac501a31bd6ad2011e36133a1": ("USDC", 6, True),
    "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near": ("USDC.e", 6, True),
}
# Calls to these contracts carry the bridge / intents detail in their logs.
PARSE_TARGETS = {"wrap.near", "omni.bridge.near", "intents.near", "aggregatedex.near"}
FINAL_1CLICK = {"SUCCESS", "REFUNDED", "FAILED"}
MAX_DEPTH = 8
SERVICE_TXCOUNT = 1000
BIG_HOP = 1000.0  # NEAR; new tracked account funded with at least this alerts

NOTIFY = True


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now()}] {msg}", flush=True)


def alert(title, msg):
    log(f"ALERT {title}: {msg}")
    if NOTIFY and sys.platform == "darwin":
        script = f'display notification {json.dumps(msg)} with title {json.dumps(title)} sound name "Glass"'
        subprocess.run(["osascript", "-e", script], capture_output=True)


def http_json(url, data=None, headers=None, timeout=30, tries=3):
    h = {"user-agent": "hacker-watch/1.0", **(headers or {})}
    body = None
    if data is not None:
        body = json.dumps(data).encode()
        h["content-type"] = "application/json"
    err = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, body, h), timeout=timeout) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return None
            err = e
        except Exception as e:  # network blips, JSON errors
            err = e
        time.sleep(2 * (i + 1))
    log(f"WARN {url[:90]}: {err}")
    return None


def pike(path):
    return http_json(PIKE + path, headers={"x-api-key": PIKE_KEY})


def rpc(method, params, url=RPC):
    r = http_json(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                  headers={"Authorization": f"Bearer {FASTNEAR_KEY}"} if FASTNEAR_KEY else None)
    if not r or "error" in r:
        return None
    return r.get("result")


def balance(acc):
    r = rpc("query", {"request_type": "view_account", "finality": "final", "account_id": acc})
    return int(r["amount"]) / 1e24 if r and "amount" in r else None


def short(a, n=10):
    return a if len(a) <= 2 * n + 1 else f"{a[:n]}…{a[-6:]}"


def asset_name(asset_id):
    """nep141:btc.omft.near -> BTC, nep141:eth-0x…omft.near -> eth:0x…"""
    a = (asset_id or "").split(":", 1)[-1]
    if a.endswith(".omft.near"):
        base = a[: -len(".omft.near")]
        return base.upper() if "-" not in base else base.replace("-", ":", 1)
    return TOKENS[a][0] if a in TOKENS else a


# --------------------------------------------------------------------- state

SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts(
  id TEXT PRIMARY KEY, depth INT, parent TEXT, kind TEXT, label TEXT,
  first_seen TEXT, balance REAL, dirty INT DEFAULT 3);
CREATE TABLE IF NOT EXISTS transfers(
  tx TEXT, sender TEXT, receiver TEXT, amount REAL, ts INT,
  PRIMARY KEY(tx, sender, receiver, amount));
CREATE TABLE IF NOT EXISTS exits(
  key TEXT PRIMARY KEY, via TEXT, src TEXT, deposit_addr TEXT, tx TEXT,
  amount_near REAL, amount_usd REAL, dest_asset TEXT, recipient TEXT,
  amount_out TEXT, status TEXT, dest_tx TEXT, ts INT, updated TEXT);
CREATE TABLE IF NOT EXISTS parsed_txs(tx TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS btc(addr TEXT PRIMARY KEY, received REAL, spent REAL, txs INT, updated TEXT);
CREATE TABLE IF NOT EXISTS evm(addr TEXT, chain TEXT, native REAL, near_token REAL, nonce INT, updated TEXT,
  PRIMARY KEY(addr, chain));
"""


def open_db():
    db = sqlite3.connect(os.path.join(HERE, "watch.db"))
    db.row_factory = sqlite3.Row
    db.executescript(SCHEMA)
    if not db.execute("SELECT 1 FROM accounts WHERE id=?", (ROOT,)).fetchone():
        db.execute("INSERT INTO accounts VALUES(?,?,?,?,?,?,?,?)",
                   (ROOT, 0, None, "root", "hacker wallet", now(), None, 3))
    db.commit()
    return db


# ------------------------------------------------------------ classification

def oneclick_status(addr):
    r = http_json(ONECLICK + addr)
    return r if r and r.get("status") else None


def record_oneclick(db, addr, src, tx, st):
    q = (st.get("quoteResponse") or {}).get("quoteRequest") or {}
    sd = st.get("swapDetails") or {}
    quote = (st.get("quoteResponse") or {}).get("quote") or {}
    dest_txs = ",".join(h["hash"] for h in sd.get("destinationChainTxHashes") or [])
    key = f"1click:{addr}"
    old = db.execute("SELECT status FROM exits WHERE key=?", (key,)).fetchone()
    amt_in = float(sd.get("amountInFormatted") or quote.get("amountInFormatted") or 0)
    usd = float(sd.get("amountInUsd") or quote.get("amountInUsd") or 0)
    out = sd.get("amountOutFormatted") or quote.get("amountOutFormatted")
    db.execute(
        "INSERT INTO exits VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET "
        "status=excluded.status, amount_out=excluded.amount_out, dest_tx=excluded.dest_tx, "
        "amount_usd=excluded.amount_usd, updated=excluded.updated",
        (key, "1Click/Intents", src, addr, tx, amt_in, usd, asset_name(q.get("destinationAsset")),
         q.get("recipient"), out, st["status"], dest_txs, None, now()))
    if not old:
        alert("Exit via Intents",
              f"{amt_in:,.0f} NEAR (${usd:,.0f}) → {out} {asset_name(q.get('destinationAsset'))} "
              f"to {q.get('recipient')} [{st['status']}]")
    elif old["status"] != st["status"]:
        log(f"1Click {short(addr)} status {old['status']} -> {st['status']}")


def forwards_to_cex(acc, rows=None):
    """CEX hot wallet that takes >= FORWARD_SHARE of acc's outgoing NEAR, if any."""
    rows = rows if rows is not None else (pike(f"/account/near-transfer/{acc}?limit=50") or [])
    out = {}
    for r in rows:
        if isinstance(r, dict) and r.get("sender") == acc and r.get("status", True):
            out[r["receiver"]] = out.get(r["receiver"], 0) + float(r["amount"])
    total = sum(out.values())
    if total <= 0:
        return None
    best = max(out, key=out.get)
    return best if best in CEX_HOT and out[best] >= FORWARD_SHARE * total else None


def venue_of(acc, hot=None):
    """(route, label, contact) when acc is a known or detected venue deposit address."""
    if acc in VENUE_DEPOSITS:
        return VENUE_DEPOSITS[acc]
    if hot:
        name = CEX_HOT[hot]
        return (name, f"{name} deposit address (forwards to {short(hot, 6)})", name)
    return None


def classify(db, acc, src, tx):
    if acc in SERVICES:
        return "service", SERVICES[acc]
    if acc in VENUE_DEPOSITS:
        return "venue", VENUE_DEPOSITS[acc][1]
    if acc in CEX_HOT:
        return "cex", f"{CEX_HOT[acc]} hot wallet"
    hot = forwards_to_cex(acc)
    if hot:
        return "venue", venue_of(acc, hot)[1]
    st = oneclick_status(acc)
    if st:
        record_oneclick(db, acc, src, tx, st)
        return "oneclick", "1Click deposit address"
    tc = pike(f"/account/tx-count/{acc}")
    if isinstance(tc, int) and tc > SERVICE_TXCOUNT:
        return "service", f"busy account ({tc:,} txs)"
    return "tracked", ""


def update_venue_exits(db):
    """Turn cluster payments into service / exchange deposit addresses into exits.

    1. Known venue addresses and CEX hot wallets get their kind fixed (older
       runs filed them as 'service' or 'binance').
    2. A tracked account whose outgoing NEAR (as seen in transfers) goes
       >= FORWARD_SHARE to one CEX hot wallet is a deposit address of that
       venue (Exolix reuses these; Binance gives one per user): it leaves the
       cluster and becomes a 'venue'.
    3. Every cluster -> venue/cex transfer after the incident is an exit row.
    """
    for a, (route, label, _) in VENUE_DEPOSITS.items():
        db.execute("UPDATE accounts SET kind='venue', label=? WHERE id=?", (label, a))
    for a, name in CEX_HOT.items():
        db.execute("UPDATE accounts SET kind='cex', label=? WHERE id=?", (f"{name} hot wallet", a))
    db.execute("DELETE FROM exits WHERE key LIKE 'binance:%'")  # legacy rows, rebuilt below
    for acc in [r[0] for r in db.execute("SELECT id FROM accounts WHERE kind='tracked'")]:
        rows = [{"sender": s, "receiver": rc, "amount": a} for s, rc, a in
                db.execute("SELECT sender, receiver, amount FROM transfers WHERE sender=?", (acc,))]
        hot = forwards_to_cex(acc, rows)
        if hot:
            label = venue_of(acc, hot)[1]
            db.execute("UPDATE accounts SET kind='venue', label=?, dirty=0 WHERE id=?", (label, acc))
            log(f"{short(acc)} reclassified: {label}")
    venues = {r["id"]: r for r in db.execute("SELECT * FROM accounts WHERE kind IN ('venue','cex')")}
    cluster = {r[0] for r in db.execute("SELECT id FROM accounts WHERE kind IN ('root','tracked')")}
    for t in db.execute("SELECT * FROM transfers WHERE ts >= ? ORDER BY ts", (INCIDENT_NS,)).fetchall():
        if t["receiver"] not in venues or t["sender"] not in cluster:
            continue
        rcv = t["receiver"]
        if rcv in VENUE_DEPOSITS:
            route, label, _ = VENUE_DEPOSITS[rcv]
        elif rcv in CEX_HOT:
            route, label = CEX_HOT[rcv], f"{CEX_HOT[rcv]} hot wallet (direct)"
        else:
            hot = next((r[0] for r in db.execute(
                "SELECT receiver FROM transfers WHERE sender=? GROUP BY receiver ORDER BY SUM(amount) DESC LIMIT 1",
                (rcv,))), None)
            label = venues[rcv]["label"] or "Exchange deposit address"
            route = CEX_HOT.get(hot) or label.split(" deposit address")[0]
        key = f"venue:{t['tx']}:{rcv}"
        if db.execute("SELECT 1 FROM exits WHERE key=?", (key,)).fetchone():
            continue
        db.execute("INSERT INTO exits VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (key, route, t["sender"], rcv, t["tx"], t["amount"], None, "NEAR", rcv,
                    f"{t['amount']:.4f}", "SUCCESS", None, t["ts"], now()))
        alert(f"Exit via {route}", f"{t['amount']:,.2f} NEAR from {short(t['sender'])} → {label}")
    db.commit()


def venue_holds():
    """Live balances where exited NEAR is sitting (for the 'freezable' panel)."""
    return [{"address": a, "route": route, "what": label, "who_can_act": who, "balance_near": balance(a)}
            for a, (route, label, who) in VENUE_HOLDS.items()]


# ----------------------------------------------------------------- scanning

def new_transfers(db, acc):
    """Pikespeak rows for acc not yet in the DB, oldest first."""
    rows, offset = [], 0
    for _ in range(40):
        page = pike(f"/account/near-transfer/{acc}?limit=50&offset={offset}")
        if not isinstance(page, list):
            break
        fresh = []
        for r in page:
            key = (r["transaction_id"], r["sender"], r["receiver"], float(r["amount"]))
            if not db.execute("SELECT 1 FROM transfers WHERE tx=? AND sender=? AND receiver=? AND amount=?",
                              key).fetchone():
                fresh.append(r)
        rows += fresh
        if len(page) < 50 or len(fresh) < len(page):
            break
        offset += 50
    return sorted(rows, key=lambda r: int(r["timestamp"]))


def tx_time(status):
    """Block timestamp (ns) of a tx_status result; outcomes only carry the block hash."""
    bh = (status.get("transaction_outcome") or {}).get("block_hash")
    blk = rpc("block", {"block_id": bh}, ARCHIVAL) if bh else None
    return int(blk["header"]["timestamp"]) if blk else 0


def parse_tx(db, tx, sender):
    """Pull Omni InitTransferEvent / Intents mt_mint details out of a tx's logs."""
    if db.execute("SELECT 1 FROM parsed_txs WHERE tx=?", (tx,)).fetchone():
        return
    r = rpc("EXPERIMENTAL_tx_status", {"tx_hash": tx, "sender_account_id": sender,
                                        "wait_until": "EXECUTED_OPTIMISTIC"}, ARCHIVAL)
    if not r:
        return  # retry next cycle
    db.execute("INSERT INTO parsed_txs VALUES(?)", (tx,))
    ts = tx_time(r)
    for o in r.get("receipts_outcome", []):
        for line in o["outcome"]["logs"]:
            if line.startswith("EVENT_JSON:"):
                line = line[len("EVENT_JSON:"):]
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if "InitTransferEvent" in ev:
                m = ev["InitTransferEvent"]["transfer_message"]
                chain, _, to = m["recipient"].partition(":")
                key = f"omni:{m['origin_nonce']}"
                if db.execute("SELECT 1 FROM exits WHERE key=?", (key,)).fetchone():
                    continue
                token = m["token"].split(":", 1)[-1]
                label, dec, is_usd = TOKENS.get(token, (token, None, False))
                units = int(m["amount"]) / 10 ** dec if dec is not None else None
                amt = units if token == "wrap.near" else None
                db.execute("INSERT INTO exits VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (key, "Omni Bridge", sender, None, tx, amt, units if is_usd else None,
                            f"{label}@{chain}", to, None if amt is not None or units is None else f"{units:g}",
                            "INITIATED", None, ts, now()))
                alert("Exit via Omni Bridge", f"{units if units is not None else m['amount']} {label} → {chain} {to}")
            elif ev.get("standard") == "nep245" and ev.get("event") == "mt_mint":
                for d in ev.get("data", []):
                    key = f"intents:{tx}:{d['owner_id']}"
                    if db.execute("SELECT 1 FROM exits WHERE key=?", (key,)).fetchone():
                        continue
                    db.execute("INSERT INTO exits VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                               (key, "Intents deposit", sender, None, tx, None, None,
                                ",".join(asset_name(t) for t in d["token_ids"]), d["owner_id"],
                                ",".join(d["amounts"]), "DEPOSITED", None, ts, now()))
                    alert("Intents deposit", f"{short(sender)} credited intents account {d['owner_id']}")


def parse_function_calls(db, acc):
    """Contract calls (ft_transfer_call etc.) don't show in Pikespeak's NEAR
    transfers, so list them from Nearblocks and parse the interesting ones."""
    r = http_json(f"{NEARBLOCKS}/account/{acc}/txns?per_page=25")
    for t in (r or {}).get("txns", []):
        if t["predecessor_account_id"] != acc:
            continue
        # any token can be bridged: ft_transfer_call on the token contract, not just wrap.near
        if t["receiver_account_id"] in PARSE_TARGETS or any(
                a.get("method") == "ft_transfer_call" for a in t.get("actions") or []):
            parse_tx(db, t["transaction_hash"], acc)
    time.sleep(10)  # Nearblocks free tier is ~6 req/min


def scan(db, acc):
    me = db.execute("SELECT * FROM accounts WHERE id=?", (acc,)).fetchone()
    rows = new_transfers(db, acc)
    touched_contracts = False
    for r in rows:
        snd, rcv, amt = r["sender"], r["receiver"], float(r["amount"])
        db.execute("INSERT OR IGNORE INTO transfers VALUES(?,?,?,?,?)",
                   (r["transaction_id"], snd, rcv, amt, int(r["timestamp"])))
        if snd != acc or rcv == "system":
            continue
        if acc == ROOT:
            alert("ROOT WALLET MOVED", f"{amt:,.2f} NEAR → {short(rcv)}")
        if rcv in PARSE_TARGETS:
            touched_contracts = True
        if db.execute("SELECT 1 FROM accounts WHERE id=?", (rcv,)).fetchone():
            continue
        kind, label = classify(db, rcv, acc, r["transaction_id"])
        depth = me["depth"] + 1
        if kind == "tracked" and depth > MAX_DEPTH:
            kind, label = "terminal", "depth limit"
        db.execute("INSERT INTO accounts VALUES(?,?,?,?,?,?,?,?)",
                   (rcv, depth, acc, kind, label, now(), None, 3 if kind == "tracked" else 0))
        # venue / cex payments become exits in update_venue_exits()
        if kind == "tracked":
            msg = f"{short(acc)} → {short(rcv)} {amt:,.2f} NEAR (depth {depth})"
            alert("New hop", msg) if amt >= BIG_HOP else log("new hop " + msg)
    # paying any named contract (storage deposits included) means it may have bridged or swapped tokens
    if touched_contracts or db.execute(
            "SELECT 1 FROM transfers WHERE sender=? AND receiver LIKE '%.%' LIMIT 1", (acc,)).fetchone():
        parse_function_calls(db, acc)
    db.commit()
    return len(rows)


def refresh_open_exits(db):
    for e in db.execute("SELECT key, deposit_addr, src, tx FROM exits WHERE via='1Click/Intents' "
                        "AND status NOT IN ('SUCCESS','REFUNDED','FAILED')").fetchall():
        st = oneclick_status(e["deposit_addr"])
        if st:
            record_oneclick(db, e["deposit_addr"], e["src"], e["tx"], st)
    db.commit()


def refresh_btc(db):
    addrs = {r[0] for r in db.execute("SELECT recipient FROM exits WHERE dest_asset='BTC'")}
    for a in addrs:
        r = http_json(MEMPOOL + a)
        if not r:
            continue
        cs, ms = r["chain_stats"], r["mempool_stats"]
        db.execute("INSERT OR REPLACE INTO btc VALUES(?,?,?,?,?)",
                   (a, (cs["funded_txo_sum"] + ms["funded_txo_sum"]) / 1e8,
                    (cs["spent_txo_sum"] + ms["spent_txo_sum"]) / 1e8, cs["tx_count"] + ms["tx_count"], now()))
    db.commit()


def evm_chain(asset):
    """Which EVM chain an exit's dest_asset lands on: ETH, eth:0x…, wNEAR@eth, BNB…"""
    a = (asset or "").lower()
    if "@" in a:
        a = a.split("@", 1)[1]
    a = a.split(":", 1)[0]
    return {"eth": "eth", "base": "base", "arb": "arb", "bsc": "bnb", "bnb": "bnb"}.get(a)


def evm_call(url, method, params):
    r = http_json(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    return int(r["result"], 16) if r and r.get("result") not in (None, "0x") else 0


def refresh_evm(db):
    seen = set()
    for e in db.execute("SELECT dest_asset, recipient FROM exits WHERE recipient LIKE '0x%'"):
        chain = evm_chain(e["dest_asset"])
        if not chain or (e["recipient"].lower(), chain) in seen:
            continue
        seen.add((e["recipient"].lower(), chain))
        url, near_token = EVM[chain]
        a = e["recipient"]
        bal_of = "0x70a08231" + a[2:].lower().rjust(64, "0")
        db.execute("INSERT OR REPLACE INTO evm VALUES(?,?,?,?,?,?)",
                   (a, chain, evm_call(url, "eth_getBalance", [a, "latest"]) / 1e18,
                    evm_call(url, "eth_call", [{"to": near_token, "data": bal_of}, "latest"]) / 1e24,
                    evm_call(url, "eth_getTransactionCount", [a, "latest"]), now()))
    db.commit()


# ------------------------------------------------------------------ report

def write_report(db):
    acc = {r["id"]: r for r in db.execute("SELECT * FROM accounts")}
    root_bal = acc[ROOT]["balance"]
    root_out = db.execute("SELECT COALESCE(SUM(amount),0) FROM transfers WHERE sender=?", (ROOT,)).fetchone()[0]
    # rows before the incident are false positives (tracing through shared contracts)
    exits = db.execute("SELECT * FROM exits WHERE ts IS NULL OR ts = 0 OR ts >= ? ORDER BY COALESCE(ts,0), updated",
                       (INCIDENT_NS,)).fetchall()
    sitting = [r for r in acc.values() if r["kind"] == "tracked" and (r["balance"] or 0) > 1]

    L = [f"# Hacker wallet watch", "",
         f"Updated {now()} UTC · root [`{short(ROOT)}`](https://pikespeak.ai/account/{ROOT}?tab=transfers)", "",
         "| | NEAR |", "|---|---:|",
         f"| Funded by Binance (Oct 29) | {ROOT_FUNDED:,.0f} |",
         f"| Root balance now | {root_bal:,.0f} |" if root_bal is not None else "| Root balance now | ? |",
         f"| Sent out by root | {root_out:,.0f} |",
         f"| Sitting in downstream wallets | {sum(r['balance'] or 0 for r in sitting):,.0f} |",
         f"| Accounts in cluster | {sum(1 for r in acc.values() if r['kind'] in ('root', 'tracked'))} |", ""]

    by = {}
    for e in exits:
        k = (e["via"], e["dest_asset"])
        b = by.setdefault(k, [0, 0.0, 0.0])
        b[0] += 1
        b[1] += e["amount_near"] or 0
        b[2] += e["amount_usd"] or 0
    L += ["## Exits by route", "", "| Route | Asset | Count | NEAR in | USD in |", "|---|---|---:|---:|---:|"]
    L += [f"| {v} | {a} | {c} | {n:,.0f} | {u:,.0f} |" for (v, a), (c, n, u) in sorted(by.items())]
    L += ["", "## Exit log", "",
          "| Route | From | NEAR | USD | Out | Recipient | Status | Dest tx |", "|---|---|---:|---:|---|---|---|---|"]
    for e in exits:
        L.append(f"| {e['via']} | `{short(e['src'] or '', 6)}` | {e['amount_near'] or 0:,.0f} | {e['amount_usd'] or 0:,.0f} "
                 f"| {e['amount_out'] or ''} {e['dest_asset'] or ''} | `{e['recipient']}` | {e['status']} "
                 f"| `{short(e['dest_tx'] or '', 8)}` |")

    venue_rows = [e for e in exits if e["key"].startswith("venue:")]
    if venue_rows:
        L += ["", "## CEX exits through services", "",
              "Cluster payments into deposit addresses of services that end at an exchange "
              "(the swap service that banks at Binance, Exolix, exchange deposit addresses).", "",
              "| Route | Deposits | NEAR in | Deposit addresses |", "|---|---:|---:|---:|"]
        agg = {}
        for e in venue_rows:
            a = agg.setdefault(e["via"], [0, 0.0, set()])
            a[0] += 1
            a[1] += e["amount_near"] or 0
            a[2].add(e["deposit_addr"])
        L += [f"| {v} | {c} | {n:,.2f} | {len(s)} |" for v, (c, n, s) in sorted(agg.items(), key=lambda kv: -kv[1][1])]
        L += ["", "### Where that NEAR sits now", "", "| Address | What | Who can act | Balance |", "|---|---|---|---:|"]
        L += [f"| `{short(h['address'])}` | {h['what']} | {h['who_can_act']} | "
              f"{h['balance_near']:,.2f} |" if h["balance_near"] is not None else
              f"| `{short(h['address'])}` | {h['what']} | {h['who_can_act']} | ? |" for h in venue_holds()]

    btc = db.execute("SELECT * FROM btc ORDER BY received DESC").fetchall()
    if btc:
        L += ["", "## BTC recipients (mempool.space)", "",
              "| Address | Received | Spent | Held | Txs |", "|---|---:|---:|---:|---:|"]
        L += [f"| [`{b['addr']}`](https://mempool.space/address/{b['addr']}) | {b['received']:.5f} | {b['spent']:.5f} "
              f"| {b['received'] - b['spent']:.5f} | {b['txs']} |" for b in btc]

    L += ["", "## Downstream wallets still holding NEAR", "", "| Account | Depth | Funded by | Balance |",
          "|---|---:|---|---:|"]
    L += [f"| `{r['id']}` | {r['depth']} | `{short(r['parent'] or '', 6)}` | {r['balance']:,.2f} |"
          for r in sorted(sitting, key=lambda r: -(r["balance"] or 0))]
    L += ["", "## All accounts", "", "| Account | Kind | Depth | Funded by | Balance | Note |", "|---|---|---:|---|---:|---|"]
    for r in sorted(acc.values(), key=lambda r: (r["depth"], r["first_seen"])):
        bal = f"{r['balance']:,.2f}" if r["balance"] is not None else ""
        L.append(f"| `{short(r['id'])}` | {r['kind']} | {r['depth']} | `{short(r['parent'] or '', 6)}` | {bal} | {r['label'] or ''} |")

    with open(os.path.join(HERE, "report.md"), "w") as f:
        f.write("\n".join(L) + "\n")
    write_export(db, acc, exits, btc)
    with open(os.path.join(HERE, "edges.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["utc", "tx", "sender", "receiver", "amount_near"])
        for t in db.execute("SELECT * FROM transfers ORDER BY ts"):
            w.writerow([datetime.fromtimestamp(t["ts"] / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        t["tx"], t["sender"], t["receiver"], t["amount"]])


def write_export(db, acc, exits, btc):
    """JSON row files for the claude.ai dashboard (export/*.json)."""
    out = os.path.join(HERE, "export")
    os.makedirs(out, exist_ok=True)
    sent = dict(db.execute("SELECT sender, SUM(amount) FROM transfers GROUP BY sender").fetchall())
    tx_ts = dict(db.execute("SELECT tx, MIN(ts) FROM transfers GROUP BY tx").fetchall())
    utc = lambda ns: datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if ns else ""
    received = dict(db.execute("SELECT receiver, SUM(amount) FROM transfers GROUP BY receiver").fetchall())
    stamp = now() + "Z"
    files = {
        "accounts.json": [{"account": r["id"], "kind": r["kind"], "depth": r["depth"],
                           "funded_by": r["parent"] or "", "balance_near": round(r["balance"] or 0, 4),
                           "received_near": round(received.get(r["id"], 0), 4),
                           "sent_near": round(sent.get(r["id"], 0), 4), "note": r["label"] or "",
                           "first_seen": r["first_seen"], "as_of": stamp}
                          for r in acc.values()],
        "exits.json": [{"exit_id": e["key"], "time_utc": utc(e["ts"] or tx_ts.get(e["tx"])),
                        "route": e["via"], "from_account": e["src"] or "",
                        "amount_near": round(e["amount_near"] or 0, 4), "amount_usd": round(e["amount_usd"] or 0, 2),
                        "asset_out": e["dest_asset"] or "", "amount_out": e["amount_out"] or "",
                        "recipient": e["recipient"] or "", "status": e["status"] or "",
                        "near_tx": e["tx"] or "",
                        "deposit_address": e["deposit_addr"] if e["via"] == "1Click/Intents"
                        or e["key"].startswith("venue:") else "",
                        "dest_tx": e["dest_tx"] or "", "as_of": stamp} for e in exits],
        "btc.json": [{"address": b["addr"], "received_btc": b["received"], "spent_btc": b["spent"],
                      "held_btc": round(b["received"] - b["spent"], 8), "txs": b["txs"], "as_of": b["updated"]}
                     for b in btc],
    }
    evm_rows = {}
    for e in exits:
        chain = evm_chain(e["dest_asset"])
        if not chain or not (e["recipient"] or "").startswith("0x") or e["status"] in ("FAILED", "REFUNDED"):
            continue
        r = evm_rows.setdefault((e["recipient"].lower(), chain), {
            "address": e["recipient"], "chain": chain, "assets": set(), "routes": set(),
            "exits": 0, "near_in": 0.0, "usd_in": 0.0})
        r["assets"].add(e["dest_asset"])
        r["routes"].add(e["via"])
        r["exits"] += 1
        r["near_in"] += e["amount_near"] or 0
        r["usd_in"] += e["amount_usd"] or 0
    bal = {(b["addr"].lower(), b["chain"]): b for b in db.execute("SELECT * FROM evm")}
    files["evm.json"] = []
    for key, r in evm_rows.items():
        b = bal.get(key)
        files["evm.json"].append({
            "address": r["address"], "chain": r["chain"], "assets_received": ", ".join(sorted(r["assets"])),
            "routes": ", ".join(sorted(r["routes"])), "exits": r["exits"], "near_in": round(r["near_in"], 4),
            "usd_in": round(r["usd_in"], 2),
            "native_balance": round(b["native"], 6) if b else None,
            "near_token_balance": round(b["near_token"], 4) if b else None,
            "outgoing_txs": b["nonce"] if b else None, "as_of": b["updated"] if b else ""})
    # CEX exit routes through services: totals per route plus live balances
    # of the addresses where that NEAR is still sitting (freezable).
    routes = {}
    for e in exits:
        if not e["key"].startswith("venue:"):
            continue
        r = routes.setdefault(e["via"], {"route": e["via"], "deposits": 0, "near_in": 0.0,
                                         "deposit_addresses": set(), "first_utc": None, "last_utc": None})
        r["deposits"] += 1
        r["near_in"] += e["amount_near"] or 0
        r["deposit_addresses"].add(e["deposit_addr"])
        t = utc(e["ts"])
        r["first_utc"] = min(filter(None, [r["first_utc"], t]))
        r["last_utc"] = max(filter(None, [r["last_utc"], t]))
    holds = venue_holds()
    files["venues.json"] = [
        {**r, "near_in": round(r["near_in"], 4), "deposit_addresses": len(r["deposit_addresses"]),
         "held_near": round(sum(h["balance_near"] or 0 for h in holds if h["route"] == r["route"]), 4)
         if any(h["route"] == r["route"] for h in holds) else None,
         "who_can_act": VENUE_CONTACTS.get(r["route"], r["route"]),
         "as_of": stamp} for r in routes.values()]
    files["holds.json"] = [{**h, "balance_near": round(h["balance_near"], 4) if h["balance_near"] is not None else None,
                            "as_of": stamp} for h in holds]
    for name, rows in files.items():
        with open(os.path.join(out, name), "w") as f:
            json.dump(rows, f, indent=0)


# -------------------------------------------------------------------- main

def cycle(db, n):
    scanned = 0
    accounts = db.execute("SELECT * FROM accounts WHERE kind IN ('root','tracked')").fetchall()
    with ThreadPoolExecutor(8) as pool:
        balances = dict(zip([a["id"] for a in accounts], pool.map(balance, [a["id"] for a in accounts])))
    for a in accounts:
        bal = balances[a["id"]]
        changed = bal is not None and (a["balance"] is None or abs(bal - a["balance"]) > 1e-6)
        if a["id"] == ROOT and changed and a["balance"] is not None:
            alert("Root balance changed", f"{a['balance']:,.2f} → {bal:,.2f} NEAR")
        dirty = 3 if changed else a["dirty"]
        if dirty > 0:
            found = scan(db, a["id"])
            scanned += 1
            dirty = 0 if found else dirty - 1
        db.execute("UPDATE accounts SET balance=COALESCE(?,balance), dirty=? WHERE id=?", (bal, dirty, a["id"]))
        db.commit()
    refresh_open_exits(db)
    update_venue_exits(db)
    unseen_btc = db.execute("SELECT 1 FROM exits WHERE dest_asset='BTC' AND recipient NOT IN "
                            "(SELECT addr FROM btc) LIMIT 1").fetchone()
    if n % 10 == 0 or unseen_btc:
        refresh_btc(db)
    unseen_evm = db.execute("SELECT 1 FROM exits WHERE recipient LIKE '0x%' AND lower(recipient) NOT IN "
                            "(SELECT lower(addr) FROM evm) LIMIT 1").fetchone()
    if n % 10 == 0 or unseen_evm:
        refresh_evm(db)
    write_report(db)
    return scanned


def main():
    global NOTIFY
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=120)
    ap.add_argument("--no-notify", action="store_true")
    args = ap.parse_args()
    NOTIFY = not args.no_notify
    if not PIKE_KEY:
        sys.exit("PIKESPEAK_KEY is not set: copy .env.example to .env and add your key")
    db = open_db()
    n = 0
    while True:
        s = 0
        try:
            s = cycle(db, n)
            tracked = db.execute("SELECT COUNT(*) FROM accounts WHERE kind IN ('root','tracked')").fetchone()[0]
            ex = db.execute("SELECT COUNT(*) FROM exits").fetchone()[0]
            log(f"cycle {n}: scanned {s}, cluster {tracked}, exits {ex}")
        except Exception as e:  # keep the watcher alive through API hiccups
            log(f"ERROR cycle {n}: {e!r}")
        n += 1
        if args.once:
            # --once keeps walking until the newly found hops are scanned too
            if s == 0 or n >= 15:
                break
            continue
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
