import json
import unittest

import measure_live_rag_recall as measure


def output(exact, planner, eligibility):
    sections = []
    for name, plan in (("EXACT", exact), ("PLANNER", planner), ("ELIGIBILITY", eligibility)):
        sections.append(f"LIVE_RAG_{name}_PLAN_BEGIN\n{json.dumps(plan)}\nLIVE_RAG_{name}_PLAN_END")
    sections.append('LIVE_RAG_METRICS={"recall_at_k":0.5}')
    return "\n".join(sections) + "\n"


class RecallTests(unittest.TestCase):
    def test_vector_validation(self):
        self.assertEqual(measure.vector_arg("[1,0,0]", 3), "[1,0,0]")
        for value in ("[0,0,0]", "[1,2]", "[1,NaN,2]", "[true,1,2]", "[1e999,1,2]"):
            with self.assertRaises(ValueError):
                measure.vector_arg(value, 3)

    def test_reports_planner_choice_separately_from_eligibility(self):
        exact = [{"Plan": {"Node Type": "Seq Scan"}}]
        index = [{"Plan": {"Node Type": "Index Scan", "Index Name": "application_ann"}}]
        result = measure.parse_output(output(exact, exact, index), "application_current")
        self.assertEqual(result["metrics"]["recall_at_k"], 0.5)
        self.assertFalse(result["planner_used_ann_index"])
        self.assertTrue(result["forced_plan_used_ann_index"])
        with self.assertRaisesRegex(ValueError, "exact baseline"):
            measure.parse_output(output(index, exact, index), "application_current")
        with self.assertRaises(ValueError):
            measure.parse_output("missing", "application_current")


if __name__ == "__main__":
    unittest.main()
