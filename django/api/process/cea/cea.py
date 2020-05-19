from api.process.cea.models.row import Row
from api.process.cea.linkage import Linkage
from api.process.utils.table import Table


class CEAProcess:
    # TODO: What data is table??
    # TODO: I need rows and column analysis results
    def __init__(self, table, tags: list, normalized_map: dict, candidates_map: dict):
        self._table = table
        self._normalized_map = normalized_map   # { <original_cell>: <norm_cell> }
        self._candidates_map = candidates_map   # { <norm_cell>: [(<label>, <entity>), (<label>, <entity>),...] }
        self._tags = tags                       # e.g. [SUBJ, NE, LIT, NE]

    def compute(self):
        results = []

        # TODO: Do I really need enumeration??
        for row_idx, table_row in enumerate(self._table.get_rows()):
            row = self._build_row(table_row)
            if row.get_subject_cell() is None:
                # TODO: This is a serious error. What should I do?
                print("WARNING: row has no subject column. Ignoring...")
                continue
                    
            table_rm = Linkage(row)
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
            cands = self._candidates_map.get(norm, [])

            if self._is_necol(self._table, pos):
                is_subject = self._is_subject(self._table, pos)
                row.add_ne_cell(cell, norm, cands, is_subject=is_subject)
            else:
                # TODO: Is cands always empty list???
                row.add_lit_cell(cell, cell, cands) # TODO: literal normalization???

        return row

    def _is_necol(self, table, pos):
        assert (pos >= 0 and pos < len(self._tags))
        return  self._tags[pos] != "LIT"

    def _is_subject(self, table, pos):
        assert (pos >= 0 and pos < len(self._tags))
        #return  self._tags[pos] == "SUBJ"      # TODO
        return pos == 0