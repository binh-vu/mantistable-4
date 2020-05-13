class Cell:
    def __init__(self, content: str, normalized: str, candidates: list, is_lit_cell=False):
        self.content = content
        self.normalized = normalized
        self.is_lit_cell = is_lit_cell

        self._cands_entities = [
            cand[1]
            for cand in candidates
        ]
        self._cands_labels = {}
        for label, uri in candidates:
            if uri not in self._cands_labels:
                self._cands_labels[uri] = []
            self._cands_labels[uri].append(label)

    def candidates_entities(self):
        return self._cands_entities

    def candidates_labels(self, entity):
        return self._cands_labels.get(entity, [])