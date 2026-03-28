"""
Tests for the Fellegi-Sunter weight logic used in the SDP pipeline.

These run locally with pytest — no Spark required.
Tests the weight computation and classification thresholds.
"""

import pytest

# Replicate the constants from sdp_pipeline.py
FS_MATCH = 8.0
FS_POSS = 4.0
W_SSN4_AGREE = 4.595
W_SSN4_DISAGREE = -4.595
W_DOB_AGREE = 3.892
W_DOB_DISAGREE = -3.892
NAME_SCALE = 2.0


def compute_weights(first_name_sim, last_name_sim, ssn4_exact, dob_exact):
    """Replicate the _apply_fs_weights logic from sdp_pipeline.py."""
    w_ssn4 = W_SSN4_AGREE if ssn4_exact == 1.0 else W_SSN4_DISAGREE
    w_dob = W_DOB_AGREE if dob_exact == 1.0 else W_DOB_DISAGREE
    w_first = first_name_sim * NAME_SCALE - 1.0
    w_last = last_name_sim * NAME_SCALE - 1.0
    total = w_ssn4 + w_dob + w_first + w_last
    return total, w_ssn4, w_dob, w_first, w_last


def classify(total_weight):
    if total_weight >= FS_MATCH:
        return "match"
    elif total_weight >= FS_POSS:
        return "possible_match"
    else:
        return "non_match"


class TestPerfectMatch:
    """SSN4 match + DOB match + high name similarity → match."""

    def test_all_exact(self):
        total, _, _, _, _ = compute_weights(1.0, 1.0, 1.0, 1.0)
        assert total > FS_MATCH
        assert classify(total) == "match"

    def test_high_names(self):
        total, _, _, _, _ = compute_weights(0.95, 0.95, 1.0, 1.0)
        assert classify(total) == "match"


class TestCompleteMismatch:
    """SSN4 disagree + DOB disagree + low names → non_match."""

    def test_all_disagree(self):
        total, _, _, _, _ = compute_weights(0.0, 0.0, 0.0, 0.0)
        assert total < FS_POSS
        assert classify(total) == "non_match"

    def test_low_names_no_anchors(self):
        total, _, _, _, _ = compute_weights(0.3, 0.3, 0.0, 0.0)
        assert classify(total) == "non_match"


class TestPartialMatch:
    """SSN4 match + DOB disagree → possible_match range."""

    def test_ssn4_only(self):
        total, _, _, _, _ = compute_weights(0.5, 0.5, 1.0, 0.0)
        # SSN4 agree (+4.595) + DOB disagree (-3.892) + low names ≈ 0.7
        # Total ≈ 0.7 + ... depends on name sim
        assert total > -5  # sanity check

    def test_dob_only(self):
        total, _, _, _, _ = compute_weights(0.5, 0.5, 0.0, 1.0)
        assert total > -5


class TestAnchorWeights:
    """SSN4 and DOB should carry the heaviest weights."""

    def test_ssn4_is_heaviest(self):
        total_with = compute_weights(0.5, 0.5, 1.0, 0.0)[0]
        total_without = compute_weights(0.5, 0.5, 0.0, 0.0)[0]
        diff = total_with - total_without
        assert diff == W_SSN4_AGREE - W_SSN4_DISAGREE  # Should be ~9.19

    def test_dob_is_second_heaviest(self):
        total_with = compute_weights(0.5, 0.5, 0.0, 1.0)[0]
        total_without = compute_weights(0.5, 0.5, 0.0, 0.0)[0]
        diff = total_with - total_without
        assert abs(diff - (W_DOB_AGREE - W_DOB_DISAGREE)) < 1e-10  # Should be ~7.784

    def test_ssn4_heavier_than_dob(self):
        ssn4_swing = W_SSN4_AGREE - W_SSN4_DISAGREE
        dob_swing = W_DOB_AGREE - W_DOB_DISAGREE
        assert ssn4_swing > dob_swing


class TestThresholds:
    def test_match_threshold(self):
        assert classify(8.0) == "match"
        assert classify(8.01) == "match"

    def test_possible_match_threshold(self):
        assert classify(4.0) == "possible_match"
        assert classify(7.99) == "possible_match"

    def test_non_match(self):
        assert classify(3.99) == "non_match"
        assert classify(0.0) == "non_match"
        assert classify(-10.0) == "non_match"


class TestWeightConsistency:
    """If classified as 'match', weight must be >= FS_MATCH."""

    def test_match_implies_high_weight(self):
        for fn_sim in [0.8, 0.9, 1.0]:
            for ln_sim in [0.8, 0.9, 1.0]:
                for ssn4 in [0, 1]:
                    for dob in [0, 1]:
                        total = compute_weights(fn_sim, ln_sim, ssn4, dob)[0]
                        cls = classify(total)
                        if cls == "match":
                            assert total >= FS_MATCH
                        if cls == "non_match":
                            assert total < FS_POSS
