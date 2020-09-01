from api.process.cea.models.row import Row
from api.process.cea.linkage import Linkage
from api.process.utils.table import Table
from api.process.utils.rules import person_rule as rules

class CEAProcess:
    def __init__(self, table, tags: list, normalized_map: dict, candidates_map: dict): #normalized_map: dict, candidates_map: dict):
        self._table = table
        #self._triples = triples
        self._normalized_map = normalized_map   # { <original_cell>: <norm_cell> }
        self._candidates_map = candidates_map   # { <norm_cell>: [(<label>, <entity>), (<label>, <entity>),...] }
        self._tags = tags                       # e.g. [SUBJ, NE, LIT, NE]

    def compute(self, lamapi_backend):
        results = []
        
        for table_row in self._table.get_rows():
            row = self._build_row(table_row)
            if row.get_subject_cell() is None:
                # TODO: This is a serious error. What should I do?
                print("WARNING: row has no subject column. Ignoring...")
                continue
                    
            table_rm = Linkage(row, lamapi_backend)
            links = table_rm.get_links()
            subjects = table_rm.get_subjects(links)
            results.append(
                (subjects, links)
            )

        return results

    def _build_row(self, table_row):
        row = Row()
        for pos, cell in enumerate(table_row.values()):
            norm = self._normalized_map.get(cell, None)


            """ TODO
            rule = rules.PersonRule(cell)
            if rule.match():
                query = rule.build_query()
            else:
                query = norm
            """
            query = norm

            cands = self._candidates_map.get(query, [])

            if self._is_necol(self._table, pos):
                is_subject = self._is_subject(self._table, pos)
                row.add_ne_cell(cell, norm, cands, is_subject=is_subject)
            else:
                row.add_lit_cell(cell, cell, [])

        return row

    def _is_necol(self, table, pos):
        assert (pos >= 0 and pos < len(self._tags))
        return  self._tags[pos] != "LIT"

    def _is_subject(self, table, pos):
        assert (pos >= 0 and pos < len(self._tags))
        return  self._tags[pos] == "SUBJ"