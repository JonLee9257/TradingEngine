"""Unit tests for hybrid STRATEGY#LATEST promotion gates."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_root = Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from models.strategy_promotion import (  # noqa: E402
    PromotionGateConfig,
    evaluate_promotion_gates,
)


class TestPromotionGates(unittest.TestCase):
    def test_all_pass(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.75,
            old_threshold=0.70,
            article_count=25,
            config=cfg,
        )
        self.assertTrue(r.approved)
        self.assertEqual(r.failures, [])

    def test_min_threshold_fails(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.60,
            old_threshold=0.65,
            article_count=30,
            config=cfg,
        )
        self.assertFalse(r.approved)
        self.assertTrue(any("min_threshold" in f for f in r.failures))

    def test_max_threshold_fails(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.90,
            old_threshold=0.80,
            article_count=30,
            config=cfg,
        )
        self.assertFalse(r.approved)
        self.assertTrue(any("max_threshold" in f for f in r.failures))

    def test_jump_fails_at_boundary(self) -> None:
        cfg = PromotionGateConfig(max_jump_absolute=0.15)
        r = evaluate_promotion_gates(
            new_threshold=0.85,
            old_threshold=0.70,
            article_count=30,
            config=cfg,
        )
        self.assertFalse(r.approved)
        self.assertTrue(any("max_jump" in f for f in r.failures))

    def test_jump_passes_strictly_inside(self) -> None:
        cfg = PromotionGateConfig(max_jump_absolute=0.15)
        r = evaluate_promotion_gates(
            new_threshold=0.74,
            old_threshold=0.70,
            article_count=30,
            config=cfg,
        )
        self.assertTrue(r.approved)

    def test_no_old_threshold_skips_jump_check(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.75,
            old_threshold=None,
            article_count=25,
            config=cfg,
        )
        self.assertTrue(r.approved)

    def test_sample_size_fails(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.75,
            old_threshold=0.70,
            article_count=20,
            config=cfg,
        )
        self.assertFalse(r.approved)
        self.assertTrue(any("sample_size" in f for f in r.failures))

    def test_multiple_failures_reported(self) -> None:
        cfg = PromotionGateConfig()
        r = evaluate_promotion_gates(
            new_threshold=0.50,
            old_threshold=0.70,
            article_count=5,
            config=cfg,
        )
        self.assertFalse(r.approved)
        self.assertGreaterEqual(len(r.failures), 2)


if __name__ == "__main__":
    unittest.main()
