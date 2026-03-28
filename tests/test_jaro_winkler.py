"""
Tests for the pure-Python Jaro-Winkler implementation used in the SDP pipeline.

These run locally with pytest — no Spark required.
Import the functions directly from the pipeline module.
"""

import pytest

# Pure-Python Jaro-Winkler implementation (same as in sdp_pipeline.py)
# Duplicated here for local testing without Spark dependency.

def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if not l1 or not l2:
        return 0.0
    match_distance = max(max(l1, l2) // 2 - 1, 0)
    s1_matches = [False] * l1
    s2_matches = [False] * l2
    matches = 0
    transpositions = 0
    for i in range(l1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, l2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(l1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / l1 + matches / l2 + (matches - transpositions / 2) / matches) / 3.0


def _jaro_winkler(a: str, b: str, prefix_weight: float = 0.1) -> float:
    if not a or not b:
        return 0.0
    a = a.lower().strip()
    b = b.lower().strip()
    if a == b:
        return 1.0
    jaro_sim = _jaro(a, b)
    prefix_len = 0
    for c1, c2 in zip(a[:4], b[:4]):
        if c1 == c2:
            prefix_len += 1
        else:
            break
    return min(jaro_sim + prefix_len * prefix_weight * (1.0 - jaro_sim), 1.0)


def jw(a, b):
    return float(_jaro_winkler(a or "", b or ""))


class TestJaroWinklerIdentical:
    def test_identical(self):
        assert jw("MARTHA", "MARTHA") == 1.0

    def test_identical_lowercase(self):
        assert jw("john", "john") == 1.0

    def test_identical_mixed_case(self):
        assert jw("John", "JOHN") == 1.0


class TestJaroWinklerSimilar:
    def test_martha_marhta(self):
        assert jw("MARTHA", "MARHTA") > 0.9

    def test_mcdonald_macdonald(self):
        assert jw("McDonald", "MacDonald") > 0.8

    def test_johnson_jonson(self):
        assert jw("Johnson", "Jonson") > 0.9

    def test_smith_smyth(self):
        assert jw("Smith", "Smyth") > 0.8

    def test_obrien_variants(self):
        assert jw("O'Brien", "OBrien") > 0.85


class TestJaroWinklerDifferent:
    def test_completely_different(self):
        assert jw("ABC", "XYZ") < 0.5

    def test_swapped_names(self):
        assert jw("Robert", "Williams") < 0.6


class TestJaroWinklerEdgeCases:
    def test_none_first(self):
        assert jw(None, "John") == 0.0

    def test_none_second(self):
        assert jw("John", None) == 0.0

    def test_both_none(self):
        assert jw(None, None) == 0.0

    def test_empty_first(self):
        assert jw("", "John") == 0.0

    def test_empty_second(self):
        assert jw("John", "") == 0.0

    def test_both_empty(self):
        assert jw("", "") == 0.0

    def test_single_char_same(self):
        assert jw("J", "J") == 1.0

    def test_single_char_diff(self):
        score = jw("A", "Z")
        assert 0.0 <= score <= 1.0


class TestJaroWinklerPrefixBoost:
    def test_prefix_boost_exists(self):
        """Strings sharing a common prefix should score higher than Jaro alone."""
        jw_score = _jaro_winkler("DWAYNE", "DUANE")
        jaro_score = _jaro("dwayne", "duane")
        assert jw_score >= jaro_score

    def test_no_prefix_no_boost(self):
        """No common prefix → JW equals Jaro."""
        jw_score = _jaro_winkler("XYZ", "ABC")
        jaro_score = _jaro("xyz", "abc")
        assert abs(jw_score - jaro_score) < 0.01


class TestJaroWinklerRange:
    """All scores must be in [0, 1]."""
    @pytest.mark.parametrize("a,b", [
        ("John", "Jane"), ("A", "ABCDEFGHIJ"),
        ("Test", "Testing"), ("XX", "YY"),
        ("Mary-Jo", "MaryJo"), ("De La Cruz", "DeLaCruz"),
    ])
    def test_score_range(self, a, b):
        score = jw(a, b)
        assert 0.0 <= score <= 1.0, f"Score {score} out of range for ({a}, {b})"
