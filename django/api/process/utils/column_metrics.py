from api.process.utils.nlp import utils

def uc(values: list) -> float:
    """
        Fraction of cells with unique content
    """

    if len(values) == 0:
        return 0.0

    unique_values = set(values)
    return len(unique_values) / len(values)


def aw(values: list) -> float:
    """
        Average number of words in each cell
    """
    if len(values) == 0:
        return 0.0

    cleaned = utils.remove_punctuations(" ".join(values))
    cleaned = utils.remove_extra_spaces(cleaned)

    # This is ok only for english corpus
    words = utils.remove_stopwords(cleaned.split(' '))
    number_of_words = len(words)

    return number_of_words / len(values)
    

def emc(values: list) -> float:
    """
        Fraction of empty cells
    """

    if len(values) == 0:
        return 0.0

    empty_cells_count = sum([
        1
        for v in values
        if len(v.strip()) == 0
    ])

    return empty_cells_count / len(values)


def df(col_idx: int, first_ne_idx: int) -> float:
    """
        Distance from the first NE-column
    """
    return abs(float(col_idx - first_ne_idx))
