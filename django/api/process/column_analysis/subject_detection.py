import math

from api.process.utils.table import Table
from api.process.utils.column_metrics import uc, aw, emc, df

def _subject_score(uc_norm: float, aw_norm: float, emc_norm: float, df: float) -> float:
    """
        Main subject detection formula
    """
    return (2.0*uc_norm + aw_norm - emc_norm) / math.sqrt(df + 1.0)


def _compute_subjects_scores(columns: dict, first_ne_idx: int) -> list:
    """
        Compute metrics and subjects score for every columns
    """
    assert (first_ne_idx >= 0)

    # uc, aw, emc, df
    # ...
    scores = [
        (
            uc(values),
            aw(values),
            emc(values),
            df(col_idx, first_ne_idx)
        )
        for col_idx, (_header, values) in enumerate(columns.items())
    ]

    # uc, uc, uc, uc, ...
    # aw, aw, aw, aw, ...
    # ...
    scores_transposed = list(map(list, zip(*scores)))
    assert (len(scores_transposed) == 4)

    max_scores = [
        max(scores)
        for scores in scores_transposed
    ]

    subjects_scores = []
    for uc_s, aw_s, emc_s, df_s in scores:
        uc_norm  = uc_s  / max(max_scores[0], 1)
        aw_norm  = aw_s  / max(max_scores[1], 1)
        emc_norm = emc_s / max(max_scores[2], 1)
        subjects_scores.append(
            _subject_score(uc_norm, aw_norm, emc_norm, df_s)
        )

    return subjects_scores


def get_subject_col_idx(table: Table, metadatas: list) -> int:
    """
        Given a table and its metadatas gives the most probable subject column index
    """
    def argmax(iterable):
        return max(enumerate(iterable), key=lambda x: x[1])[0]

    cols_type = [
        meta["tags"]["col_type"]
        for meta in metadatas
    ]

    try:
        first_ne_idx = cols_type.index("NE")
    except ValueError:
        # This should never happens 'cos it would mean there is no subject cell (not a good table)
        # Superimpose zero as first NE index
        first_ne_idx = 0

    ss = _compute_subjects_scores(table.get_cols(), first_ne_idx)
    subject_col_idx = argmax(ss)
    
    return subject_col_idx
