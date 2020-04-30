class Cell:
    def __init__(self, content: str, normalized: str, candidates: list, is_lit_cell=False):
        self.content = content
        self.normalized = normalized
        self.candidates = candidates
        self.is_lit_cell = is_lit_cell