from api.process.utils.lamapi.wrapper import LamAPIWrapper
import asyncio
import aiohttp
from aiohttp import ClientSession, TCPConnector

from api.process.normalization import normalizer, cleaner, cleaner_light
import os
import mantistable
import json

class CandidatesRetrieval:
    def __init__(self, cells, lamapi_backend):
        self._cells = set(cells)

        self._wrapper = LamAPIWrapper(
            lamapi_backend["host"],
            lamapi_backend["port"],
            lamapi_backend["accessToken"],
        )

    def get_candidates(self):
        return asyncio.run(self._get_candidates())

    def write_candidates_cache(self):
        return asyncio.run(self._write_candidates_cache())

    def get_cells(self):
        return self._cells

    async def _get_candidates(self):
        lock = asyncio.Lock()

        candidates = {}
        connector = TCPConnector(limit=200)
        async with ClientSession(connector=connector) as session:
            total = len(self._cells)
            for idx, cell in enumerate(self._cells):
                print(f"{idx}/{total}")
                response = await self._wrapper.labels(f'{cell}', session)
                
                result = []
                if "hits" in response:
                    result = [
                        item["_source"]
                        for item in response["hits"]["hits"]
                    ]
                    
                    if len(result) == 0:
                        response = await self._wrapper.labels_fuzzy(f'{cell}', session)
                        if "hits" in response:
                            result = [
                                item["_source"]
                                for item in response["hits"]["hits"]
                            ]
                
                """
                if "results" in response:
                    result = response["results"]
                else:
                    result = []
                """
                async with lock:
                    candidates[cell] = result

        return candidates

    async def _write_candidates_cache(self):
        cache = CacheWriter()
        connector = TCPConnector(limit=200)
        async with ClientSession(connector=connector) as session:
            total = len(self._cells)
            for idx, cell in enumerate(self._cells):
                print(f"{idx}/{total}")
                """
                response = await self._wrapper.labels_fuzzy(f'{cell}', session)
                if "hits" in response:
                    result = [
                        item["_source"]
                        for item in response["hits"]["hits"]
                    ]
                else:
                    result = []
                """
                response = await self._wrapper.labels(f'{cell}', session)
                
                result = []
                if "hits" in response:
                    result = [
                        item["_source"]
                        for item in response["hits"]["hits"]
                    ]
                    
                    if len(result) == 0:
                        response = await self._wrapper.labels_fuzzy(f'{cell}', session)
                        if "hits" in response:
                            result = [
                                item["_source"]
                                for item in response["hits"]["hits"]
                            ]
                        else:
                            result = []
                else:
                    result = []
                """
                if "results" in response:
                    result = response["results"]
                else:
                    result = []
                """
                cache.add(cell, result)
                #candidates[cell] = result

        cache.write_index()


class CacheWriter:
    def __init__(self):
        # Reset cache files
        self.cmapfile = os.path.join(mantistable.settings.MEDIA_ROOT, "candidates.map")
        self.cidxfile = os.path.join(mantistable.settings.MEDIA_ROOT, "candidates.index")
        if os.path.exists(self.cmapfile):
            os.remove(self.cmapfile)

        if os.path.exists(self.cidxfile):
            os.remove(self.cidxfile)

        self.candidates_index = {}
        self.last_offset = 0

    def add(self, cell, results):
        data_retrieval_result = []

        for res in results:
            label = res["label"]
            entity = res["uri"]

            norm_label = cleaner_light.CleanerLight(label).clean()           
            data_retrieval_result.append((norm_label, entity))

        with open(self.cmapfile, "a") as f:
            content = json.dumps(data_retrieval_result)
            content = content + "\n"
            f.write(content)
            self.candidates_index[cell] = (self.last_offset, len(content))
            self.last_offset += len(content)

    def write_index(self):
        with open(self.cidxfile, "w") as f_idx:
            json.dump(self.candidates_index, f_idx)


"""
if __name__ == "__main__":
    import json
    with FuturesSession() as session:
        lapiw = CandidatesRetrieval(
            [
                "batman", "nolan", "pfister"
            ],    
            {
                "host": "149.132.176.50",
                "port": 8097,
                "accessToken": "ee4ba0c4f8db0eb3580cb3b7b5536c54"
            }
        )
        res = lapiw.get_candidates()
        print(json.dumps(res, indent=4))
        print(len(res))
"""
