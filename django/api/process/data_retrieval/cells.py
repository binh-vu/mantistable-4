from api.process.utils.lamapi.wrapper import LamAPIWrapper
import asyncio
import aiohttp
from aiohttp import ClientSession, TCPConnector

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
                response = await self._wrapper.labels(f'"{cell}"', session)
                
                if "hits" in response:
                    result = [
                        item["_source"]
                        for item in response["hits"]["hits"]
                    ]
                else:
                    result = []
                
                """
                if "results" in response:
                    result = response["results"]
                else:
                    result = []
                """
                async with lock:
                    candidates[cell] = result

        return candidates


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