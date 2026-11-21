import json
import os
import subprocess
import time
import live_rag_load

env = os.environ.copy()
env["PGAPPNAME"] = "rag_timeout_checker"
subprocess.run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", "CREATE TABLE IF NOT EXISTS public.rag_timeout_probe (id int)"], env=env, check=True, capture_output=True, text=True)
os.environ["PGAPPNAME"] = "rag_timeout_reader"

def hold_lock():
    with subprocess.Popen(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", "BEGIN; LOCK TABLE public.rag_timeout_probe IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(35); COMMIT"], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as lock:
        time.sleep(32)
        active = subprocess.run(["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", "SELECT count(*), count(*) FILTER (WHERE query_start < clock_timestamp() - interval '30 seconds') FROM pg_stat_activity WHERE application_name = 'rag_timeout_reader'"], env=env, check=True, capture_output=True, text=True).stdout.strip()
        count, overdue = (int(value) for value in active.split("|"))
        assert count <= 1 and overdue == 0, active
        _, errors = lock.communicate(timeout=10)
        assert lock.returncode == 0, errors
        return {"active_readers_while_locked": count, "overdue_readers_while_locked": overdue}

result, reads, _ = live_rag_load.measure_during(hold_lock, "SELECT json_build_object('server_started',extract(epoch from statement_timestamp()), 'server_finished',extract(epoch from clock_timestamp()), 'rows',count(*))::text FROM public.rag_timeout_probe", {}, 1)
timeouts = [r for r in reads if r.get("timed_out")]
assert len(timeouts) == 1, reads
assert timeouts[0]["timeout_source"] == "server_statement_timeout", timeouts
remaining = subprocess.run(["psql", "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'rag_timeout_reader'"], env=env, check=True, capture_output=True, text=True).stdout.strip()
assert remaining == "0", remaining
print(json.dumps({**result, "timed_out_readers": len(timeouts), "timeout_source": timeouts[0]["timeout_source"], "remaining_reader_backends": int(remaining), "observations": len(reads)}))
