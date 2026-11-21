#!/usr/bin/env python3
"""Run targeted pipeline regressions against a release binary (requires polars)."""

import csv
from datetime import date
from io import StringIO
import os
from pathlib import Path
import subprocess
import sys
import tempfile

from run_fixtures import SCHEMAS, load_actual


EVENTS = ("event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents")
CLICKS = ("click_id", "user_id", "campaign_id", "ts")
AT = "2026-10-01T10:00:00Z"
DAY_TWO = "2026-10-02T10:00:00Z"


def csv_bytes(rows, quoted=False):
    out = StringIO(newline="")
    csv.writer(out, lineterminator="\n", quoting=csv.QUOTE_ALL if quoted else csv.QUOTE_MINIMAL).writerows(rows)
    return out.getvalue().encode("utf-8")


def pad_to(buffer, target):
    # All filler lines have one field and therefore contribute no records.
    fill = b"?" * 255 + b"\n"
    n, tail = divmod(target - len(buffer), len(fill))
    buffer.extend(fill * n)
    if tail:
        buffer.extend(b"?" * (tail - 1) + b"\n")


def event(key, person, price="", product="p1", ts=AT, ingest=AT):
    return key, person, ts, ingest, "purchase" if price else "view", product, price


def make_input(data):
    for folder in ("events", "clicks", "links"):
        (data / folder).mkdir(parents=True)
    (data / "manifest.json").write_text(
        '{"window_start":"2026-10-01T00:00:00Z","window_end":"2026-10-05T00:00:00Z"}'
    )
    (data / "products.csv").write_bytes(csv_bytes([
        ("product_id", "category"), ("p1", "猫"), ("p01", "padded"),
    ]))
    (data / "campaigns.csv").write_bytes(csv_bytes([
        ("campaign_id", "channel"), ("c_1", "search"), ("c_01", "email"),
    ]))

    canonical = "A / 身份\0⭐"
    short_alias = "u12345678"
    long_alias = "设备" + "x" * 48
    user_prefix = "different/用户/" + "ab" * 70
    user_a, user_b = user_prefix + "A", user_prefix + "B"
    id_prefix = "generic-id/猫/" + "q" * 4600
    keep, drop = id_prefix + "earliest-valid", id_prefix + "earliest-invalid"
    old, later = "1800-01-01T00:00:00Z", "1850-01-01T00:00:00Z"

    (data / "links" / "0.csv").write_bytes(csv_bytes([
        ("user_id_a", "user_id_b"), (short_alias, long_alias),
    ]))
    (data / "links" / "1.csv").write_bytes(csv_bytes([
        ("user_id_a", "user_id_b"), (canonical, long_alias), (long_alias, short_alias),
    ], quoted=True).removesuffix(b"\n"))

    records = bytearray(csv_bytes([EVENTS,
        event(id_prefix + "A", short_alias, "17", "p01"),
        # Both copies fall outside the packed timestamp interval. The later
        # copies arrive first so treating their ingest times as equal fails.
        event(keep, user_a, "999", ts="bad", ingest=later),
        event(drop, user_b, "999", ingest=later),
        event("x", user_a),
    ]))
    # Long distinct IDs share their first 4 KiB. The two records below start
    # just before chunk boundaries and their raw pointers live across reads.
    pad_to(records, 256 * 1024 - 7)
    records.extend(csv_bytes([
        event(id_prefix + "B", long_alias, "23"),
        event(drop, canonical, "99", ts="bad", ingest=old),
        event("x\0", user_a),
        event("\0" * 9, user_b, "11", "P1"),
        event("e1", "uabc"),
    ], quoted=True))
    pad_to(records, 512 * 1024 - 3)
    records.extend(csv_bytes([
        event(keep, canonical, "5", "p01", ingest=old),
        event(id_prefix + "A", short_alias, "17", "p01"),
        event("a2345678", "uabc\0", "13"),
        event("d2", user_a, ts=DAY_TWO, ingest=DAY_TWO),
    ]))
    (data / "events" / "large.csv").write_bytes(records)
    # Eligible size for pread, but the final record needs an added newline.
    eof = bytearray(csv_bytes([EVENTS]))
    pad_to(eof, 260 * 1024 - 5)
    eof.extend(csv_bytes([event("a23456789", "uabc\0\0", "7")]).removesuffix(b"\n"))
    (data / "events" / "eof.csv").write_bytes(eof)

    # Appending a synthetic newline must work on either side of an mmap
    # page boundary, including a last byte in an otherwise unmapped page.
    for size in (4095, 4096, 4097):
        row = csv_bytes([event(f"page{size}", "uabc")]).removesuffix(b"\n")
        page = bytearray(csv_bytes([EVENTS]))
        pad_to(page, size - len(row))
        (data / "events" / f"page-{size}.csv").write_bytes(page + row)

    clicks = bytearray(csv_bytes([CLICKS, (id_prefix + "b", long_alias, "c_01", AT)]))
    pad_to(clicks, 256 * 1024 - 1)
    clicks.extend(csv_bytes([(id_prefix + "a", short_alias, "c_1", AT)], quoted=True))
    (data / "clicks" / "0.csv").write_bytes(clicks)
    (data / "clicks" / "1.csv").write_bytes(csv_bytes([
        CLICKS, (id_prefix + "a", short_alias, "c_1", AT),
    ]).removesuffix(b"\n"))

    first, second = date(2026, 10, 1), date(2026, 10, 2)
    return {
        "user_daily": [
            (canonical, first, 1, 3, 45, "猫"),
            (user_a, first, 1, 2, 0, None), (user_a, second, 1, 1, 0, None),
            (user_b, first, 1, 1, 11, "unknown"),
            ("uabc", first, 1, 4, 0, None), ("uabc\0", first, 1, 1, 13, "猫"),
            ("uabc\0\0", first, 1, 1, 7, "猫"),
        ],
        "campaign_daily": [
            ("c_1", "search", first, 1, 1, 45, 1), ("c_01", "email", first, 0, 0, 0, 1),
            ("organic", "organic", first, 5, 3, 31, 0), ("organic", "organic", second, 1, 0, 0, 0),
        ],
    }


def main():
    if len(sys.argv) != 2:
        sys.exit(f"Usage: {sys.argv[0]} PATH_TO_RELEASE_BINARY")
    binary = Path(sys.argv[1]).resolve(strict=True)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        data = root / "data"
        expected = make_input(data)
        products = data / "products.csv"
        initial_products = products.read_bytes()
        cases = [
            ("pread", 1, []), ("pread", 4, []), ("mmap", 4, []),
            ("pread", 1, [(f"extra{i}", f"category{i}") for i in range(257)]),
            ("pread", 1, [("extra", "c" * 65536)]),
        ]
        for case, (mode, threads, extra_categories) in enumerate(cases):
            # Both catalog fallbacks must preserve every observed category,
            # including its UTF-8 bytes and the nulls on non-purchasing days.
            products.write_bytes(initial_products + csv_bytes(extra_categories))
            out = root / f"out-{case}"
            env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(out),
                       NCPU=str(threads), SS_CHUNK="4096", SS_SUB="64", SS_READ=mode)
            subprocess.run([str(binary)], env=env, check=True)
            for table, schema in SCHEMAS.items():
                actual = sorted(load_actual(out / table, schema).rows())
                if actual != sorted(expected[table]):
                    raise AssertionError(f"{mode}/{threads} {table}: expected {sorted(expected[table])!r}, got {actual!r}")
            print(f"PASS {mode} / {threads} worker(s) / {len(extra_categories)} extra categories")


if __name__ == "__main__":
    main()
