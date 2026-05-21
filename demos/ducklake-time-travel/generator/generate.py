#!/usr/bin/env python3
"""
Event generator for the DuckLake time-travel demo (Demo B).

Inserts synthetic events into the events table in periodic batches.
After INJECT_BUG_AFTER_BATCH batches (if non-zero), starts mixing in
BUG_EVENT_TYPE rows so the stream table visibly corrupts — letting you
demonstrate the pause → restore_from_snapshot → resume time-travel rewind.
"""

import os
import random
import time

import psycopg

DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/timetravel_demo")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "2000"))
INJECT_BUG_AFTER_BATCH = int(os.getenv("INJECT_BUG_AFTER_BATCH", "0"))
BUG_EVENT_TYPE = os.getenv("BUG_EVENT_TYPE", "CORRUPT_BILLING")

EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]

def generate_batch(conn, batch_num: int, inject_bug: bool):
    with conn.cursor() as cur:
        rows = []
        for i in range(BATCH_SIZE):
            if inject_bug and random.random() < 0.3:
                event_type = BUG_EVENT_TYPE
            else:
                event_type = random.choice(EVENT_TYPES)
            rows.append((
                batch_num * BATCH_SIZE + i,
                event_type,
                random.randint(1, 1000),
            ))
        cur.executemany(
            "INSERT INTO events (event_id, event_type, user_id) VALUES (%s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            rows,
        )
    conn.commit()
    bug_label = " [BUG INJECTED]" if inject_bug else ""
    print(f"[batch {batch_num}] inserted {BATCH_SIZE} events{bug_label}", flush=True)

def main():
    print(
        f"Starting event generator: batch_size={BATCH_SIZE}, interval_ms={INTERVAL_MS}"
        + (f", bug injection after batch {INJECT_BUG_AFTER_BATCH}" if INJECT_BUG_AFTER_BATCH else ""),
        flush=True,
    )
    with psycopg.connect(DSN) as conn:
        batch_num = 0
        while True:
            try:
                inject_bug = INJECT_BUG_AFTER_BATCH > 0 and batch_num >= INJECT_BUG_AFTER_BATCH
                generate_batch(conn, batch_num, inject_bug)
                batch_num += 1
            except Exception as exc:
                print(f"Error on batch {batch_num}: {exc}", flush=True)
            time.sleep(INTERVAL_MS / 1000.0)

if __name__ == "__main__":
    main()
