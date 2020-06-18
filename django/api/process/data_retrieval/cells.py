from mantistable.settings import LAMAPI_HOST, LAMAPI_PORT
from api.process.utils.lamapi.wrapper import LamAPIWrapper

class CandidatesRetrieval:
    def __init__(self, cells):
        self._cells = cells
        self._wrapper = LamAPIWrapper(LAMAPI_HOST, LAMAPI_PORT)

    def get_candidates(self):
        candidates = {}
        for norm, original in self._cells:
            if norm not in candidates:
                cands = self._wrapper.labels(norm)
                if len(cands) == 0:
                    cands = self._wrapper.labels(original)

                candidates[norm] = cands

        return candidates

    def get_cells(self):
        return self._cells