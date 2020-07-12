from api.process.utils.lamapi.wrapper import LamAPIWrapper

class CandidatesRetrieval:
    def __init__(self, cells, lamapi_backend):
        self._cells = cells
        self._wrapper = LamAPIWrapper(lamapi_backend["host"], lamapi_backend["port"], lamapi_backend["accessToken"])

    def get_candidates(self):
        candidates = {}
        for norm, original in self._cells:
            candidates[norm] = self._wrapper.labels(norm, original)

        return candidates

    def get_cells(self):
        return self._cells