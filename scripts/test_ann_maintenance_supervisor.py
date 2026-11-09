import os
import unittest
from unittest.mock import Mock, patch

import ann_maintenance_supervisor as supervisor


class SupervisorTests(unittest.TestCase):
    def test_page_validation_requires_ordered_integer_ids_and_boolean_decisions(self):
        output = '{"id":2,"eligible":false}\n{"id":5,"eligible":true}\n'
        self.assertEqual([row["id"] for row in supervisor.parse_page(output, 1, 2)], [2, 5])
        for bad in (
            '{"id":1,"eligible":true}',
            '{"id":true,"eligible":true}',
            '{"id":2,"eligible":"true"}',
            '{"id":3,"eligible":false}\n{"id":2,"eligible":true}',
        ):
            with self.assertRaises(ValueError):
                supervisor.parse_page(bad, 1, 2)
        with self.assertRaises(ValueError):
            supervisor.parse_page(output, 0, 1)

    def test_configuration_enforces_capacity_and_retry_limits(self):
        with patch.dict(os.environ, {"PGS_ANN_MAX_FAILURES": "6"}):
            self.assertEqual(supervisor.bounded_env("PGS_ANN_MAX_FAILURES", 6, 1, 6), 6)
        for value in ("0", "7", "not-a-number"):
            with patch.dict(os.environ, {"PGS_ANN_MAX_FAILURES": value}), self.assertRaises(ValueError):
                supervisor.bounded_env("PGS_ANN_MAX_FAILURES", 6, 1, 6)

    def test_supervisor_runs_only_eligible_ids(self):
        page = Mock(returncode=0)
        page.communicate.return_value = ('{"id":2,"eligible":false}\n{"id":5,"eligible":true}\n', None)
        worker = Mock()
        worker.wait.return_value = 0
        with patch.object(supervisor.signal, "signal"), patch.object(supervisor.subprocess, "Popen", side_effect=[page, worker]) as spawn, patch.object(supervisor.time, "sleep", side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                supervisor.main()
        page.communicate.assert_called_once_with(supervisor.PAGE_SQL)
        self.assertEqual(spawn.call_count, 2)
        self.assertIn("request_id=5", spawn.call_args.args[0])

    def test_stop_interrupts_psql_to_cancel_the_server_query(self):
        handlers = {}
        child = Mock(returncode=1)
        child.poll.return_value = None

        def communicate(_sql):
            handlers[supervisor.signal.SIGTERM](supervisor.signal.SIGTERM, None)
            return "", None

        child.communicate.side_effect = communicate
        with (
            patch.object(supervisor.signal, "signal", side_effect=lambda number, handler: handlers.update({number: handler})),
            patch.object(supervisor.subprocess, "Popen", return_value=child),
        ):
            self.assertEqual(supervisor.main(), 0)
        child.send_signal.assert_called_once_with(supervisor.signal.SIGINT)


if __name__ == "__main__":
    unittest.main()
