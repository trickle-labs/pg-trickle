#!/usr/bin/env python3
"""
Event generator for the DuckLake time-travel demo (Demo B).

Inserts synthetic events into the DuckLake foreign table in periodic batches.
Each batch advances the DuckLake snapshot by one increment.
"""

import os
import random
import time

import psycopg

DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/timetravel_demo")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
INTERVAL_MS = int(os.getenv("INTERVAL_MS", "2000"))

EVENT_TYPES = ["page_view", "click", "purchase", "signup", "logout"]

def generate_batch(conn, batch_num: int):
    with conn.cursor() as cur:
        rows = [
            (
                batch_num * BATCH_SIZE + i,
                random.choice(EVENT_TYPES),
                random.randint(1, 1000),
            )
            for i in range(BATCH_SIZE)
        ]
        cur.executemany(
            "INSERT INTO events (event_id, event_type, user_id) VALUES (%s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            rows,
        )
    conn.commit()
    print(f"[batch {batch_num}] inserted {BATCH_SIZE} events", flush=True)

def main():
    print(f"Starting event generator: batch_size={BATCH_SIZE}, interval_ms={INTERVAL_MS}", flush=True)
    with psycopg.connect(DSN) as conn:
        batch_num = 0
        while True:
            try:
                generate_batch(conn, batch_num)
                batch_num += 1
            except Exception as exc:
                print(f"Error on batch {batch_num}: {exc}", flush=True)
            time.sleep(INTERVAL_MS / 1000.0)

if __name__ == "__main__":
    main()
