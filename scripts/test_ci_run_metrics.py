import unittest

from ci_run_metrics import summarize


class MetricsTests(unittest.TestCase):
    def test_queue_execution_and_skipped_jobs_are_separate(self):
        run = {
            "status": "completed", "conclusion": "success", "html_url": "fixture",
            "head_sha": "abc", "event": "pull_request",
        }
        job = {
            "name": "suite", "conclusion": "success",
            "created_at": "2026-10-18T10:00:00Z", "started_at": "2026-10-18T10:03:00Z",
            "completed_at": "2026-10-18T10:05:00Z",
        }
        result = summarize(run, [job, {"conclusion": "skipped"}])
        self.assertEqual(result["active_jobs"], 1)
        self.assertEqual(result["creation_to_start_minutes"], 3)
        self.assertEqual(result["execution_minutes"], 2)
        self.assertEqual(result["conclusion"], "success")
        run["conclusion"] = "failure"
        self.assertEqual(summarize(run, [job])["conclusion"], "failure")
        run["status"] = "in_progress"
        with self.assertRaisesRegex(ValueError, "partial timings"):
            summarize(run, [job])
