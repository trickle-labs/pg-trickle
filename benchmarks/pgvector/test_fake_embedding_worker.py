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

    def test_drain_counts_stale_and_duplicate_completions(self):
        claim = json.dumps({"claimed_job": "job", "claimed_token": "token", "source_body": "A"})
        with patch.object(worker, "psql", side_effect=[claim, "f", "f", ""]) as call:
            result = worker.drain("model-v1", 60, 0, 2, True)
        self.assertEqual(result, {"claimed": 1, "accepted": 0, "stale": 1, "duplicate_rejected": 1})
        self.assertEqual(call.call_args_list[0].kwargs["model"], "model-v1")

    def test_duplicate_acceptance_is_an_error(self):
        claim = json.dumps({"claimed_job": "job", "claimed_token": "token", "source_body": "A"})
        with patch.object(worker, "psql", side_effect=[claim, "t", "t"]), self.assertRaises(ValueError):
            worker.drain("model-v1", 60, 0, 1, True)


if __name__ == "__main__":
    unittest.main()
