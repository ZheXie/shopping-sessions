#!/usr/bin/env python3
"""
Shopping Sessions pipeline (correctness baseline).

Implements rules.md exactly: custom CSV dialect, linked IDs, sessions,
attribution/assists, and both daily Parquet outputs.
"""
from __future__ import annotations

import glob
import json
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional

NCPU = os.environ.get("NCPU")
if NCPU:
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import polars as pl  # noqa: E402

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")

DAY_MS = 86_400_000
SEVEN_DAYS_MS = 7 * DAY_MS
THIRTY_MIN_MS = 1_800_000
GAP_SPLIT_MS = THIRTY_MIN_MS  # new session when gap > 30 min


# ---------------------------------------------------------------------------
# CSV dialect (rules.md): \n ends lines; \r is data; quotes wrap whole fields
# ---------------------------------------------------------------------------

def iter_data_lines(path: str) -> Iterable[str]:
    with open(path, "rb") as f:
        raw = f.read()
    # Only \n ends a line. Keep \r inside fields.
    text = raw.decode("utf-8")
    # Split on \n; trailing empty after final \n is not a line
    if text.endswith("\n"):
        text = text[:-1]
    if not text:
        return
    for line in text.split("\n"):
        yield line


def parse_csv_line(line: str, nfields: int) -> Optional[list[Optional[str]]]:
    """Return fields or None if malformed. Empty field → None."""
    if line == "":
        return None
    fields: list[Optional[str]] = []
    i = 0
    n = len(line)
    while True:
        if i < n and line[i] == '"':
            # Quoted field: "..." only; no escapes other than whole-field quotes.
            j = i + 1
            while j < n and line[j] != '"':
                j += 1
            if j >= n:
                return None  # unclosed quote
            # After closing quote must be comma or end
            if j + 1 < n and line[j + 1] != ",":
                return None
            # Quote inside content? content is line[i+1:j]; any " already ended.
            content = line[i + 1 : j]
            fields.append(None if content == "" else content)
            i = j + 1
            if i >= n:
                break
            if line[i] != ",":
                return None
            i += 1
            continue
        # Unquoted field: no " allowed; ends at comma or EOL
        j = i
        while j < n and line[j] != ",":
            if line[j] == '"':
                return None
            j += 1
        content = line[i:j]
        fields.append(None if content == "" else content)
        i = j
        if i >= n:
            break
        # line[i] == ','
        i += 1
        # trailing comma → empty field handled by next loop iteration with i==n?
        # If i == n after comma, we need one more empty field.
        if i == n:
            fields.append(None)
            break
    if len(fields) != nfields:
        return None
    return fields


def read_csv_rows(path: str, nfields: int) -> list[list[Optional[str]]]:
    rows: list[list[Optional[str]]] = []
    first = True
    for line in iter_data_lines(path):
        if first:
            first = False
            continue  # header
        parsed = parse_csv_line(line, nfields)
        if parsed is not None:
            rows.append(parsed)
    return rows


def read_glob_rows(pattern: str, nfields: int) -> list[list[Optional[str]]]:
    rows: list[list[Optional[str]]] = []
    for path in sorted(glob.glob(pattern)):
        rows.extend(read_csv_rows(path, nfields))
    return rows


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

def _is_ascii_digits(s: str) -> bool:
    return bool(s) and all("0" <= c <= "9" for c in s)


def _parse_ymd_hms(y: int, mo: int, d: int, h: int, mi: int, s: int) -> Optional[datetime]:
    try:
        return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc)
    except ValueError:
        return None


def parse_ts(s: Optional[str]) -> Optional[int]:
    """Parse F1/F2/F3 to epoch ms UTC, or None if bad."""
    if s is None:
        return None
    # F3: digits only (leading zeros OK)
    if _is_ascii_digits(s):
        try:
            v = int(s)  # Python int unlimited; i64 overflow → outside window later
        except ValueError:
            return None
        # Fit in signed 64-bit? If not, treat as unusable sentinel outside windows
        if v > 9_223_372_036_854_775_807:
            return None  # rules: not malformed as form, but can't be in window → bad
        return v

    # F1 / F2: YYYY-MM-DDTHH:MM:SSZ or ...±HH:MM
    if len(s) < 20 or s[4] != "-" or s[7] != "-" or s[10] != "T" or s[13] != ":" or s[16] != ":":
        return None
    # No lowercase t/z; fractional seconds not allowed
    try:
        y = int(s[0:4])
        mo = int(s[5:7])
        d = int(s[8:10])
        h = int(s[11:13])
        mi = int(s[14:16])
        sec = int(s[17:19])
    except ValueError:
        return None
    # Digits check for date parts (reject fullwidth etc. already via int on ASCII)
    if not (
        _is_ascii_digits(s[0:4])
        and _is_ascii_digits(s[5:7])
        and _is_ascii_digits(s[8:10])
        and _is_ascii_digits(s[11:13])
        and _is_ascii_digits(s[14:16])
        and _is_ascii_digits(s[17:19])
    ):
        return None

    rest = s[19:]
    if rest == "Z":
        dt = _parse_ymd_hms(y, mo, d, h, mi, sec)
        if dt is None:
            return None
        return int(dt.timestamp() * 1000)

    # Offset +HH:MM or -HH:MM
    if len(rest) != 6 or rest[0] not in "+-" or rest[3] != ":":
        return None
    if not (_is_ascii_digits(rest[1:3]) and _is_ascii_digits(rest[4:6])):
        return None
    oh = int(rest[1:3])
    om = int(rest[4:6])
    if oh > 23 or om > 59:
        return None
    dt = _parse_ymd_hms(y, mo, d, h, mi, sec)
    if dt is None:
        return None
    sign = 1 if rest[0] == "+" else -1
    # Subtract offset to get UTC (F2 meaning)
    delta = timedelta(hours=sign * oh, minutes=sign * om)
    utc = dt - delta
    return int(utc.timestamp() * 1000)


def parse_ingest_ts(s: Optional[str]) -> Optional[int]:
    # Always valid F1 per rules — still parse strictly
    return parse_ts(s)


def utc_date_from_ms(ms: int) -> date:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).date()


# ---------------------------------------------------------------------------
# Union-Find for linked IDs
# ---------------------------------------------------------------------------

class UnionFind:
    __slots__ = ("parent", "rank", "min_id")

    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}
        self.min_id: dict[str, str] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            self.min_id[x] = x

    def find(self, x: str) -> str:
        p = self.parent[x]
        if p != x:
            self.parent[x] = self.find(p)
        return self.parent[x]

    def union(self, a: str, b: str) -> None:
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        ma = self.min_id[ra]
        mb = self.min_id[rb]
        m = ma if ma.encode("utf-8") <= mb.encode("utf-8") else mb
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.min_id[ra] = m
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1

    def person(self, x: str) -> str:
        self.add(x)
        return self.min_id[self.find(x)]


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def load_manifest():
    with open(os.path.join(DATA, "manifest.json")) as f:
        m = json.load(f)
    w0 = parse_ts(m["window_start"])
    w1 = parse_ts(m["window_end"])
    if w0 is None or w1 is None:
        raise SystemExit("bad manifest timestamps")
    return w0, w1


def load_products() -> dict[str, str]:
    rows = read_csv_rows(os.path.join(DATA, "products.csv"), 2)
    out: dict[str, str] = {}
    for pid, cat in rows:
        if pid is None:
            continue
        out[pid] = cat if cat is not None else "unknown"
    return out


def load_campaigns() -> dict[str, str]:
    rows = read_csv_rows(os.path.join(DATA, "campaigns.csv"), 2)
    out: dict[str, str] = {}
    for cid, channel in rows:
        if cid is None:
            continue
        out[cid] = channel if channel is not None else ""
    return out


def load_links(uf: UnionFind) -> None:
    rows = read_glob_rows(os.path.join(DATA, "links", "*.csv"), 2)
    for a, b in rows:
        if a is None or b is None:
            continue
        uf.union(a, b)


def process_events(w0: int, w1: int, uf: UnionFind, products: dict[str, str]):
    """
    Returns list of surviving events as dicts:
      person, ts, event_type, product_id, price_cents (int|None), category
    """
    raw_rows = read_glob_rows(os.path.join(DATA, "events", "*.csv"), 7)
    # Parse + drop empty event_id; keep best by min ingest_ts
    best: dict[str, tuple] = {}
    # value: (ingest_ms, user_id, ts_str, event_type, product_id, price_str)
    for event_id, user_id, ts, ingest_ts, event_type, product_id, price_cents in raw_rows:
        if event_id is None:
            continue
        ingest_ms = parse_ingest_ts(ingest_ts)
        if ingest_ms is None:
            continue  # ingest always F1; if somehow bad, skip
        prev = best.get(event_id)
        if prev is None or ingest_ms < prev[0]:
            best[event_id] = (ingest_ms, user_id, ts, event_type, product_id, price_cents)

    surviving = []
    for event_id, (ingest_ms, user_id, ts, event_type, product_id, price_cents) in best.items():
        if user_id is None or user_id == "":
            continue
        ts_ms = parse_ts(ts)
        if ts_ms is None:
            continue
        if not (w0 <= ts_ms < w1):
            continue
        if event_type not in ("view", "cart", "purchase"):
            continue
        if product_id is None or product_id == "":
            continue
        price_val = None
        if event_type == "purchase":
            if price_cents is None or not _is_ascii_digits(price_cents):
                continue
            price_val = int(price_cents)
            if price_val < 1 or price_val > 10_000_000:
                continue
        # late: ingest - ts > 24h
        if ingest_ms - ts_ms > DAY_MS:
            continue
        person = uf.person(user_id)
        cat = products.get(product_id, "unknown")
        surviving.append(
            {
                "person": person,
                "ts": ts_ms,
                "event_type": event_type,
                "product_id": product_id,
                "price_cents": price_val if event_type == "purchase" else None,
                "category": cat,
            }
        )
    return surviving


def sessionize(events: list[dict]):
    """
    Assign session_id / session_start / day per event.
    Returns events with those fields, and a list of sessions:
      {person, start, day, event_count, revenue, cat_revenue: dict[str,int], has_purchase}
    """
    by_person: dict[str, list[dict]] = defaultdict(list)
    for e in events:
        by_person[e["person"]].append(e)

    sessions = []
    sid = 0
    for person, evs in by_person.items():
        evs.sort(key=lambda e: e["ts"])
        # Distinct timestamps in order
        uniq_ts = []
        for e in evs:
            if not uniq_ts or uniq_ts[-1] != e["ts"]:
                uniq_ts.append(e["ts"])
        # Map ts -> session start
        ts_to_start: dict[int, int] = {}
        start = None
        prev = None
        for t in uniq_ts:
            if prev is None or (t - prev) > GAP_SPLIT_MS:
                start = t
            ts_to_start[t] = start  # type: ignore[assignment]
            prev = t

        # Group events by session start
        groups: dict[int, list[dict]] = defaultdict(list)
        for e in evs:
            st = ts_to_start[e["ts"]]
            e["session_start"] = st
            e["day"] = utc_date_from_ms(st)
            groups[st].append(e)

        for st, gev in groups.items():
            sid += 1
            revenue = 0
            cat_rev: dict[str, int] = defaultdict(int)
            has_purchase = False
            for e in gev:
                e["session_id"] = sid
                if e["event_type"] == "purchase" and e["price_cents"] is not None:
                    has_purchase = True
                    revenue += e["price_cents"]
                    cat_rev[e["category"]] += e["price_cents"]
            sessions.append(
                {
                    "session_id": sid,
                    "person": person,
                    "start": st,
                    "day": utc_date_from_ms(st),
                    "events": len(gev),
                    "revenue": revenue,
                    "cat_rev": dict(cat_rev),
                    "has_purchase": has_purchase,
                }
            )
    return events, sessions


def process_clicks(w0: int, w1: int, uf: UnionFind, campaigns: dict[str, str]):
    raw = read_glob_rows(os.path.join(DATA, "clicks", "*.csv"), 4)
    seen: dict[str, tuple] = {}
    # click_id -> (user_id, campaign_id, ts_str) any copy
    for click_id, user_id, campaign_id, ts in raw:
        if click_id is None:
            continue
        if click_id not in seen:
            seen[click_id] = (user_id, campaign_id, ts)

    click_window_start = w0 - SEVEN_DAYS_MS
    out = []
    for click_id, (user_id, campaign_id, ts) in seen.items():
        if user_id is None or user_id == "":
            continue
        ts_ms = parse_ts(ts)
        if ts_ms is None:
            continue
        if not (click_window_start <= ts_ms < w1):
            continue
        if campaign_id is None or campaign_id not in campaigns:
            continue
        uf.add(user_id)
        person = uf.person(user_id)
        out.append(
            {
                "click_id": click_id,
                "person": person,
                "campaign_id": campaign_id,
                "ts": ts_ms,
                "channel": campaigns[campaign_id],
            }
        )
    return out


def attribute(sessions: list[dict], clicks: list[dict]):
    clicks_by_person: dict[str, list[dict]] = defaultdict(list)
    for c in clicks:
        clicks_by_person[c["person"]].append(c)
    for p in clicks_by_person:
        clicks_by_person[p].sort(key=lambda c: (c["ts"], c["click_id"]))

    for s in sessions:
        person = s["person"]
        start = s["start"]
        cand = []
        assists = set()
        for c in clicks_by_person.get(person, []):
            if c["ts"] > start:
                continue
            if start - c["ts"] > SEVEN_DAYS_MS:
                continue
            # within [start-7d, start]
            assists.add(c["campaign_id"])
            cand.append(c)
        if not cand:
            s["campaign_id"] = "organic"
            s["channel"] = "organic"
            s["assists"] = set()
        else:
            # latest ts; tie min click_id
            best = max(cand, key=lambda c: (c["ts"],))  # not enough for tie
            # Proper: max by (ts, then prefer smaller click_id → use custom)
            best_ts = max(c["ts"] for c in cand)
            tie = [c for c in cand if c["ts"] == best_ts]
            best = min(tie, key=lambda c: c["click_id"].encode("utf-8"))
            s["campaign_id"] = best["campaign_id"]
            s["channel"] = best["channel"]
            s["assists"] = assists
    return sessions


def aggregate_user_daily(sessions: list[dict]):
    # key: (person, day)
    buckets: dict[tuple, dict] = {}
    for s in sessions:
        key = (s["person"], s["day"])
        b = buckets.get(key)
        if b is None:
            b = {
                "user_id": s["person"],
                "day": s["day"],
                "sessions": 0,
                "events": 0,
                "revenue_cents": 0,
                "cat_rev": defaultdict(int),
            }
            buckets[key] = b
        b["sessions"] += 1
        b["events"] += s["events"]
        b["revenue_cents"] += s["revenue"]
        for cat, rev in s["cat_rev"].items():
            b["cat_rev"][cat] += rev

    rows = []
    for b in buckets.values():
        top = None
        if b["cat_rev"]:
            # max revenue; tie → byte-wise smallest category
            best_rev = max(b["cat_rev"].values())
            cats = [c for c, r in b["cat_rev"].items() if r == best_rev]
            top = min(cats, key=lambda c: c.encode("utf-8"))
        rows.append(
            {
                "user_id": b["user_id"],
                "day": b["day"],
                "sessions": b["sessions"],
                "events": b["events"],
                "revenue_cents": b["revenue_cents"],
                "top_category": top,
            }
        )
    return rows


def aggregate_campaign_daily(sessions: list[dict]):
    # credited: (campaign_id, day) -> stats
    credited: dict[tuple, dict] = {}
    assisted: dict[tuple, int] = defaultdict(int)
    channel_of: dict[str, str] = {"organic": "organic"}

    for s in sessions:
        cid = s["campaign_id"]
        day = s["day"]
        channel_of[cid] = s["channel"]
        key = (cid, day)
        b = credited.get(key)
        if b is None:
            b = {
                "campaign_id": cid,
                "channel": s["channel"],
                "day": day,
                "sessions": 0,
                "purchasing_sessions": 0,
                "revenue_cents": 0,
            }
            credited[key] = b
        b["sessions"] += 1
        if s["has_purchase"]:
            b["purchasing_sessions"] += 1
        b["revenue_cents"] += s["revenue"]

        for acid in s.get("assists") or ():
            if acid == "organic":
                continue
            assisted[(acid, day)] += 1
            # channel for assist-only campaigns: need from clicks — stored nowhere if never credited
            # Assists are always real campaign ids from clicks, so channel from campaigns map
            # We'll fill channel below if missing

    # Ensure assist-only rows exist
    # Need campaign channels for assist-only: look up from any session click channel
    # We may not have channel if campaign only assisted. Reload from global is cleaner —
    # pass campaigns dict. For now stash on sessions' click processing...
    # Fix: use channel_of filled when we see assists from clicks — store channel on assist set items
    # Simpler approach: keep assists as set of (campaign_id, channel)

    rows_map: dict[tuple, dict] = {}
    for key, b in credited.items():
        rows_map[key] = {
            **b,
            "assisted_sessions": 0 if b["campaign_id"] == "organic" else assisted.get(key, 0),
        }

    for key, cnt in assisted.items():
        cid, day = key
        if cid == "organic":
            continue
        if key not in rows_map:
            rows_map[key] = {
                "campaign_id": cid,
                "channel": channel_of.get(cid, ""),
                "day": day,
                "sessions": 0,
                "purchasing_sessions": 0,
                "revenue_cents": 0,
                "assisted_sessions": cnt,
            }
        else:
            rows_map[key]["assisted_sessions"] = cnt

    return list(rows_map.values())


def write_outputs(user_rows, campaign_rows, campaigns: dict[str, str]):
    # Fill missing channels for assist-only
    for r in campaign_rows:
        if not r["channel"]:
            if r["campaign_id"] == "organic":
                r["channel"] = "organic"
            else:
                r["channel"] = campaigns.get(r["campaign_id"], "")

    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)

    user_df = pl.DataFrame(
        {
            "user_id": [r["user_id"] for r in user_rows],
            "day": [r["day"] for r in user_rows],
            "sessions": [r["sessions"] for r in user_rows],
            "events": [r["events"] for r in user_rows],
            "revenue_cents": [r["revenue_cents"] for r in user_rows],
            "top_category": [r["top_category"] for r in user_rows],
        },
        schema={
            "user_id": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "events": pl.Int64,
            "revenue_cents": pl.Int64,
            "top_category": pl.String,
        },
    )
    camp_df = pl.DataFrame(
        {
            "campaign_id": [r["campaign_id"] for r in campaign_rows],
            "channel": [r["channel"] for r in campaign_rows],
            "day": [r["day"] for r in campaign_rows],
            "sessions": [r["sessions"] for r in campaign_rows],
            "purchasing_sessions": [r["purchasing_sessions"] for r in campaign_rows],
            "revenue_cents": [r["revenue_cents"] for r in campaign_rows],
            "assisted_sessions": [r["assisted_sessions"] for r in campaign_rows],
        },
        schema={
            "campaign_id": pl.String,
            "channel": pl.String,
            "day": pl.Date,
            "sessions": pl.Int64,
            "purchasing_sessions": pl.Int64,
            "revenue_cents": pl.Int64,
            "assisted_sessions": pl.Int64,
        },
    )

    # Ensure empty tables still have schema
    if user_df.height == 0:
        user_df = pl.DataFrame(
            schema={
                "user_id": pl.String,
                "day": pl.Date,
                "sessions": pl.Int64,
                "events": pl.Int64,
                "revenue_cents": pl.Int64,
                "top_category": pl.String,
            }
        )
    if camp_df.height == 0:
        camp_df = pl.DataFrame(
            schema={
                "campaign_id": pl.String,
                "channel": pl.String,
                "day": pl.Date,
                "sessions": pl.Int64,
                "purchasing_sessions": pl.Int64,
                "revenue_cents": pl.Int64,
                "assisted_sessions": pl.Int64,
            }
        )

    user_df.write_parquet(os.path.join(OUT, "user_daily", "part-0.parquet"))
    camp_df.write_parquet(os.path.join(OUT, "campaign_daily", "part-0.parquet"))


def main():
    w0, w1 = load_manifest()
    print(f"window: {w0} .. {w1}", file=sys.stderr)
    products = load_products()
    campaigns = load_campaigns()
    uf = UnionFind()
    load_links(uf)

    events = process_events(w0, w1, uf, products)
    _, sessions = sessionize(events)
    clicks = process_clicks(w0, w1, uf, campaigns)

    # Remap persons after all IDs seen (events/clicks may introduce IDs not in links)
    # process_* already called uf.person after add. But links loaded first — good.
    # Re-person sessions if needed: events already used person at process time.
    # IDs only in clicks get person at click time. Sessions use event persons.
    # Linked IDs: if u1 linked to u2, events by u2 get person min. Good.

    sessions = attribute(sessions, clicks)

    # Fix assist-only channels
    user_rows = aggregate_user_daily(sessions)
    campaign_rows = aggregate_campaign_daily(sessions)
    for r in campaign_rows:
        if r["campaign_id"] != "organic" and (
            not r.get("channel") or r["channel"] == ""
        ):
            r["channel"] = campaigns.get(r["campaign_id"], "")

    write_outputs(user_rows, campaign_rows, campaigns)
    print(
        f"wrote user_daily={len(user_rows)} campaign_daily={len(campaign_rows)}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
