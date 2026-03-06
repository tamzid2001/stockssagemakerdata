import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FUNCTIONS = ROOT / "functions"
if str(FUNCTIONS) not in sys.path:
    sys.path.insert(0, str(FUNCTIONS))

from chat_runtime import build_chat_prompt_messages, select_model_for_request


class PhaseZBackendUtilsTests(unittest.TestCase):
    def test_model_allowlist_validation_rejects_unlisted_model(self) -> None:
        selection = select_model_for_request(
            requested_model="gpt-4o",
            allowed_models=["gpt-5-mini", "gpt-5"],
            default_model="gpt-5-mini",
        )
        self.assertFalse(selection["requested_allowed"])
        self.assertEqual(selection["selected_model"], "gpt-5-mini")
        self.assertEqual(selection["allowed_models"], ["gpt-5-mini", "gpt-5"])

    def test_prompt_assembly_keeps_stable_prefix_before_dynamic_suffix(self) -> None:
        payload = {"marketCap": 1234, "headlines": [{"title": "Example"}]}
        prompts = build_chat_prompt_messages(
            stable_prefix="STATIC_SYSTEM_PREFIX",
            language_label="English",
            ticker="AAPL",
            question="What changed this week?",
            context_payload=payload,
        )
        self.assertTrue(prompts["system_prompt"].startswith("STATIC_SYSTEM_PREFIX"))
        self.assertIn("Ticker: AAPL", prompts["dynamic_user_prompt"])
        self.assertIn("Question: What changed this week?", prompts["dynamic_user_prompt"])
        self.assertIn("Ticker context payload:", prompts["dynamic_user_prompt"])
        self.assertNotIn("STATIC_SYSTEM_PREFIX", prompts["dynamic_user_prompt"])


if __name__ == "__main__":
    unittest.main()
