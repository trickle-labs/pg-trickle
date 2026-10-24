import unittest

from ci_run_metrics import summarize


class MetricsTests(unittest.TestCase):
    def test_queue_execution_and_skipped_jobs_are_separate(self):
        run = {
            "status": "completed", "conclusion": "success", "html_url": "fixture",
            "head_sha": "abc", "event": "pull_request",
            "created_at": "2026-10-18T09:59:00Z",
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
        self.assertEqual(result["wall_minutes"], 6)
        self.assertEqual(result["time_to_green_minutes"], 6)
        self.assertEqual(result["jobs"][0]["release_delay_minutes"], 1)
        self.assertIsNone(result["first_failed_step_minutes"])
        run["conclusion"] = "failure"
        job["steps"] = [{
            "name": "test", "conclusion": "failure",
            "started_at": "2026-10-18T10:03:00Z", "completed_at": "2026-10-18T10:04:00Z",
        }]
        failed = summarize(run, [job])
        self.assertIsNone(failed["time_to_green_minutes"])
        self.assertEqual(failed["first_failed_step_minutes"], 5)
        self.assertEqual(failed["jobs"][0]["steps"][0]["execution_minutes"], 1)
        run["conclusion"] = "cancelled"
        self.assertIsNone(summarize(run, [job])["time_to_green_minutes"])
        run["conclusion"] = "success"
        # Parallel execution adds runner cost but not wall time.
        parallel = summarize(run, [job, job])
        self.assertEqual(parallel["execution_minutes"], 4)
        self.assertEqual(parallel["wall_minutes"], 6)
        run["status"] = "in_progress"
        with self.assertRaisesRegex(ValueError, "partial timings"):
            summarize(run, [job])
