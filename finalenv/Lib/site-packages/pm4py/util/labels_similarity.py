'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see this software project's root or
visit <https://www.gnu.org/licenses/>.

Website: https://processintelligence.solutions
Contact: info@processintelligence.solutions
'''
import difflib
from typing import List, Dict, Set


# ---------------------------------------------------------------------------
# 1.  Similarity between two (unordered) label collections
# ---------------------------------------------------------------------------

def label_sets_similarity(
        labels_a: List[str],
        labels_b: List[str],
        threshold: float = 0.75,
) -> float:
    """
    Dice-style similarity ∈ [0,1] that tolerates near-matches.

    * Each label in A is greedily paired with its most similar,
      still-unmatched label in B (SequenceMatcher ratio ≥ `threshold`).
    * Similarity  =  2·|matched| / (|A| + |B|).
    """
    remaining_b: Set[str] = set(labels_b)
    matches = 0

    for la in labels_a:
        if not remaining_b:  # B exhausted
            break
        # find best candidate among the still-free labels in B
        best, score = None, 0.0
        for lb in remaining_b:
            r = difflib.SequenceMatcher(None, la, lb).ratio()
            if r > score:
                best, score = lb, r
        if score >= threshold:
            matches += 1
            remaining_b.remove(best)

    return 2 * matches / (len(labels_a) + len(labels_b)) if (labels_a or labels_b) else 1.0


# ---------------------------------------------------------------------------
# 2.  Greedy label mapping A → B (or A → A if no good match)
# ---------------------------------------------------------------------------

def map_labels(
        labels_a: List[str],
        labels_b: List[str],
        threshold: float = 0.75,
) -> Dict[str, str]:
    """
    Returns a dict {label_in_A: mapped_label}.

    * If an exact duplicate exists in B, map to that.
    * Otherwise pick the *unused* label in B with the highest similarity
      (SequenceMatcher ratio ≥ `threshold`).
    * If nothing reaches the threshold, map to itself (no change).
    """
    mapping: Dict[str, str] = {}
    used_b: Set[str] = set()

    for la in labels_a:
        # 1) perfect match first
        if la in labels_b and la not in used_b:
            mapping[la] = la
            used_b.add(la)
            continue

        # 2) best fuzzy match
        best, score = None, 0.0
        for lb in labels_b:
            if lb in used_b:
                continue
            r = difflib.SequenceMatcher(None, la, lb).ratio()
            if r > score:
                best, score = lb, r

        if best is not None and score >= threshold:
            mapping[la] = best
            used_b.add(best)
        else:
            mapping[la] = la  # fallback: keep original

    return mapping
