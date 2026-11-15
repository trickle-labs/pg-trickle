import json
import unittest
from unittest.mock import patch

import fake_embedding_worker as worker


class FakeEmbeddingWorkerTests(unittest.TestCase):
    def test_embedding_is_deterministic_versioned_and_nonzero(self):
        first = json.loads(worker.embedding("body", "model-v1"))
        self.assertEqual(first, json.loads(worker.embedding("body", "model-v1")))
        self.assertNotEqual(first, json.loads(worker.embedding("body", "model-v2")))
        self.assertEqual(len(first), 3)
        self.assertTrue(any(first))
        for dimensions in (384, 768, 1536):
            values = json.loads(worker.embedding("body", "model-v1", dimensions))
            self.assertEqual(len(values), dimensions)
            self.assertEqual(values, json.loads(worker.embedding("body", "model-v1", dimensions)))
            self.assertTrue(any(values))

    def test_drain_counts_stale_and_duplicate_completions(self):
        claim = json.dumps({"claimed_job": "job", "claimed_token": "token", "source_body": "A"})
        with patch.object(worker, "psql", side_effect=[claim, "f", "f", ""]) as call:
            result = worker.drain("model-v1", 60, 0, 2, True)
        self.assertEqual(result, {"claimed": 1, "accepted": 0, "stale": 1, "duplicate_rejected": 1,
                                  "embedding_delay_seconds": {"samples": 1, "p50": 0, "p95": 0, "p99": 0, "max": 0}})
        self.assertEqual(call.call_args_list[0].kwargs["model"], "model-v1")

    def test_duplicate_acceptance_is_an_error(self):
        claim = json.dumps({"claimed_job": "job", "claimed_token": "token", "source_body": "A"})
        with patch.object(worker, "psql", side_effect=[claim, "t", "t"]), self.assertRaises(ValueError):
            worker.drain("model-v1", 60, 0, 1, True)

    def test_uniform_delay_distribution_is_seeded_and_reported(self):
        claims = [json.dumps({"claimed_job": str(i), "claimed_token": "token", "source_body": "A"}) for i in range(4)]
        with patch.object(worker, "psql", side_effect=[value for claim in claims for value in (claim, "t")]), \
             patch.object(worker.time, "sleep") as sleep:
            result = worker.drain("model-v1", 60, 0.5, 4, False, delay_profile="uniform", delay_seed=1396)
        delays = [call.args[0] for call in sleep.call_args_list]
        self.assertEqual(result["embedding_delay_seconds"]["samples"], 4)
        self.assertAlmostEqual(result["embedding_delay_seconds"]["p50"], sorted(delays)[1])
        self.assertAlmostEqual(result["embedding_delay_seconds"]["p95"], sorted(delays)[-1])
        self.assertTrue(all(0 <= delay <= 1 for delay in delays))


if __name__ == "__main__":
    unittest.main()
