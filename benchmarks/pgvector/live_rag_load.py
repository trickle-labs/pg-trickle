"""Run bounded hybrid readers while one corpus is maintained."""

import json
import queue
import subprocess
import threading
import time


def measure_during(operation, query, variables, readers):
    stop = threading.Event()
    ready = [threading.Event() for _ in range(readers)]
    events = queue.Queue()
    errors = queue.Queue()

    def read_loop(started):
        command = ["psql", "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1"]
        for name, value in variables.items():
            command.extend(["-v", f"{name}={value}"])
        try:
            while not stop.is_set():
                start = time.monotonic()
                started.set()
                try:
                    result = subprocess.run(command, input=query, text=True, capture_output=True,
                                            check=True, timeout=30)
                except subprocess.TimeoutExpired:
                    events.put({"timed_out": True, "client_started": start,
                                "client_finished": time.monotonic()})
                    continue
                observation = json.loads(result.stdout.splitlines()[-1])
                observation["client_seconds"] = time.monotonic() - start
                events.put(observation)
                if stop.wait(0.05):
                    break
        except (OSError, ValueError, IndexError, subprocess.SubprocessError) as error:
            errors.put(error.stderr.strip()[:500] if isinstance(error, subprocess.CalledProcessError) and error.stderr else str(error))
            stop.set()
        finally:
            started.set()

    threads = [threading.Thread(target=read_loop, args=(event,), daemon=True) for event in ready]
    for thread in threads:
        thread.start()
    try:
        for event in ready:
            if not event.wait(35):
                raise ValueError("concurrent reader did not become ready")
        if not errors.empty():
            raise ValueError(f"concurrent reader failed: {errors.get()}")
        operation_started = time.monotonic()
        result = operation()
        operation_finished = time.monotonic()
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=35)
    if any(thread.is_alive() for thread in threads):
        raise ValueError("concurrent reader did not stop")
    if not errors.empty():
        raise ValueError(f"concurrent reader failed: {errors.get()}")
    observations = []
    while not events.empty():
        observations.append(events.get())
    return result, observations, (operation_started, operation_finished)
