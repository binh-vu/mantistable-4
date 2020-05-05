import requests
import json
import os
import time

from mantistable.settings import LAMAPI_HOST, LAMAPI_PORT
from utils.lamapi.wrapper import LamAPIWrapper

class SolrWrapper:
    def __init__(self, idx, cells):
        self._idx = idx
        self._cells = set(cells)
        self._wrapper = LamAPIWrapper(LAMAPI_HOST, LAMAPI_PORT)
        
    def get_cells(self):
        return self._cells

    def get_candidates(self):
        candidates = {}
        for cell in cells:
            candidates[cell] = self._wrapper.labels(cell)
        return candidates