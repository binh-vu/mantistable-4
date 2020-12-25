import requests
import json
import os
from api.process.utils.decorators import retry_on_exception
from typing import Dict, List, Tuple
from sm_unk.prelude import I

# with open("annotated_data/test_tables.json", "r") as f:
#     TEST_TABLES = json.load(f)

class LamAPIWrapper:
    table: I.ColumnBasedTable = None
    # column name is name of column in mantis not in the original table
    column_name2index: Dict[str, int] = None
    position2links: Dict[Tuple[str, str], List[str]] = None
    cell2position: Dict[str, List[Tuple[str, str]]] = None
    qnodes: Dict[str, I.QNode] = None

    @staticmethod
    def set_table(tbl, name2index, links, qnodes):
        LamAPIWrapper.table = tbl
        LamAPIWrapper.column_name2index = name2index
        LamAPIWrapper.position2links = links
        LamAPIWrapper.cell2position = {}
        LamAPIWrapper.qnodes = qnodes
    
    @staticmethod
    def track_normed_cell(row_idx, col_idx, query):
        if query not in LamAPIWrapper.cell2position:
            LamAPIWrapper.cell2position[query] = []
        LamAPIWrapper.cell2position[query].append((row_idx, col_idx))

    def __init__(self, endpoint: str, port: int, access_token: str, timeout=120):
        self._endpoint = endpoint
        self._port = port
        self._access_token = access_token
        self._timeout = timeout

    def infos(self):
        return self._make_request(
            lambda: requests.get(
                self._api_url("infos"),
                timeout=2
            )
        )
            
    # @retry_on_exception(max_retries=5, default=None)
    async def labels(self, label, session):
        positions = self.cell2position[label]
        qnode_ids = {qnode_id for pos in positions for qnode_id in self.position2links.get(pos, [])}

        # Follow this output json
        return {
            'hits': {
                'hits': [
                    {
                        '_source': {'label': self.qnodes[qnode_id].label, 'uri': qnode_id}
                    }
                    for qnode_id in qnode_ids
                ], 
            }, 
        }

    # @retry_on_exception(max_retries=5, default=None)
    async def labels_fuzzy(self, label, session):
        return await self.labels(label, session)

    # TODO:
    # Figure out what each func do: predicates, objects, literals, concepts
    # @retry_on_exception(max_retries=5, default={})
    def predicates(self, data):
        assert False, "Not implemented"

    # @retry_on_exception(max_retries=5, default={})
    def objects(self, subjects):
        if len(subjects) == 0:
            return {}

        self._log("objects", subjects)
        resp = {}
        for qnode_id in subjects:
            qnode = self.qnodes[qnode_id]
            resp[qnode_id] = {}
            for prop, stmts in qnode.props.items():
                values = []
                for stmt in stmts:
                    if stmt.value.is_qnode():
                        values.append(stmt.value.as_qnode_id())
                if len(values) > 0:
                    resp[qnode_id][prop] = values
        return resp

    # @retry_on_exception(max_retries=5, default={})
    def literals(self, subjects):
        if len(subjects) == 0:
            return {}

        self._log("literals", subjects)
        resp = {}
        for qnode_id in subjects:
            qnode = self.qnodes[qnode_id]
            resp[qnode_id] = {}
            for prop, stmts in qnode.props.items():
                values = []
                for stmt in stmts:
                    if stmt.value.is_qnode():
                        continue

                    if stmt.value.is_string() or stmt.value.is_quantity() or stmt.value.is_time() or stmt.value.is_mono_lingual_text():
                        values.append(stmt.value.value)
                    elif stmt.value.is_globe_coordinate():
                        values.append(f"{stmt.value.value['latitude']}, {stmt.value.value['longitude']}")
                    else:
                        assert False
                if len(values) > 0:
                    resp[qnode_id][prop] = values
        return resp

    # @retry_on_exception(max_retries=5, default={})
    def concepts(self, entities):
        assert False, "Not implemented"

    def _api_url(self, suburl):
        return f"http://{self._endpoint}:{self._port}/{suburl}"

    def _make_request(self, requester):
        response = requester()
        if response.status_code != 200:
            return {}

        return response.json()

    def _log(self, endpoint_name, query):
        query = str(query).replace('\n', '')
        if len(query) > 20:
            query = query[0:20] + '...'
        print(f"LAMAPI {endpoint_name} {query}")


    # Deprecated for now
    """
    def _labels_impl(self, norm, original, results=None, cursor=None):
        if results is None:
            results = []

        params = {
            "norm": norm,
            "original": original,
            "token": self._access_token
        }

        if cursor is not None:
            params.update({
                "nextCursor": cursor
            })
        
        response = self._make_request(
            lambda: requests.get(
                self._api_url("labels"),
                params=params, 
                timeout=self._timeout
            )
        )

        if "results" in response:
            results.extend(response["results"])

            if "cursor" in response and response["count"] > 0:
                cursor = response["cursor"]
                results = self._labels_impl(norm, original, results, cursor)
            
            return results
        
        return response
    """


"""
if __name__ == "__main__":
    lapiw = LamAPIWrapper("149.132.176.50", 8097)
    res = lapiw.labels("batman")
    print(json.dumps(res, indent=4))
    print(len(res))

"""
