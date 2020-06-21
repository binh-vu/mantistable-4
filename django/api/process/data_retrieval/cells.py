from mantistable.settings import LAMAPI_HOST, LAMAPI_PORT
from api.process.utils.lamapi.wrapper import LamAPIWrapper

class CandidatesRetrieval:
    def __init__(self, cells):
        self._cells = set(cells)
        self._wrapper = LamAPIWrapper(LAMAPI_HOST, LAMAPI_PORT)

    def get_candidates(self):
        candidates = {}
        for norm in self._cells:
            candidates[norm] = self._wrapper.labels(norm)

        return candidates

    def get_cells(self):
        return self._cells