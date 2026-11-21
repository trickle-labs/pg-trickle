import subprocess
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import live_rag_load


class ReaderLoadTests(unittest.TestCase):
    def test_reader_observations_and_operation_result_are_returned(self):
        response = SimpleNamespace(stdout='{"server_started":1,"server_finished":2,"rows":3}\n')
        started = threading.Event()
        def respond(*_args, **_kwargs):
            started.set()
            return response
        def operation():
            self.assertTrue(started.wait(2))
            return "done"
        with patch.object(live_rag_load.subprocess, "run", side_effect=respond):
            result, observations, window = live_rag_load.measure_during(operation, "SELECT 1", {}, 1)
        self.assertEqual(result, "done")
        self.assertLessEqual(window[0], window[1])
        self.assertGreaterEqual(len(observations), 1)
        self.assertEqual(observations[0]["rows"], 3)
        self.assertGreaterEqual(observations[0]["client_seconds"], 0)

    def test_reader_failure_aborts_measurement(self):
        failure = subprocess.CalledProcessError(1, ["psql"])
        with patch.object(live_rag_load.subprocess, "run", side_effect=failure):
            with self.assertRaisesRegex(ValueError, "concurrent reader failed"):
                live_rag_load.measure_during(lambda: "done", "SELECT 1", {}, 1)

    def test_reader_timeout_is_reported(self):
        timed_out = threading.Event()
        def timeout(*_args, **_kwargs):
            timed_out.set()
            raise subprocess.CalledProcessError(3, "psql", stderr="ERROR: canceling statement due to statement timeout")
        def operation():
            self.assertTrue(timed_out.wait(2))
        with patch.object(live_rag_load.subprocess, "run", side_effect=timeout):
            _, observations, _ = live_rag_load.measure_during(operation, "SELECT 1", {}, 1)
        self.assertTrue(any(item.get("timed_out") for item in observations))
        self.assertEqual(observations[0]["timeout_source"], "server_statement_timeout")

    def test_client_timeout_fails_the_measurement(self):
        with patch.object(live_rag_load.subprocess, "run", side_effect=subprocess.TimeoutExpired("psql", 35)):
            with self.assertRaisesRegex(ValueError, "concurrent reader failed"):
                live_rag_load.measure_during(lambda: "done", "SELECT 1", {}, 1)


if __name__ == "__main__":
    unittest.main()
