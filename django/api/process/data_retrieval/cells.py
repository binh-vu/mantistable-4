from mantistable.settings import LAMAPI_HOST, LAMAPI_PORT
from utils.lamapi.wrapper import LamAPIWrapper

class CandidatesRetrieval:
    def __init__(self, cells):
        self._cells = set(cells)
        self._wrapper = LamAPIWrapper(LAMAPI_HOST, LAMAPI_PORT)

    def get_candidates(self):
        candidates = {}
        for cell in self._cells:
            candidates[cell] = self._wrapper.labels(cell)

        return candidates

    def get_cells(self):
        return self._cells