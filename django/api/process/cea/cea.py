from api.process.cea.models.cell import Row
from api.process.cea.linkage import Linkage
from api.utils.table import Table


class CEAProcess:
    # TODO: What data is table??
    # TODO: I need rows and column analysis results
    def __init__(self, table, normalized_map: dict, candidates_map: dict):
        self._table = table
        self._normalized_map = normalized_map   # { <original_cell>: <norm_cell> }
        self._candidates_map = candidates_map   # { <norm_cell>: [<entity>, <entity>,...] }

    def compute(self):
        results = []

        # TODO: Do I need enumeration??
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
        raise NotImplementedError()

    def _is_subject(self, table, pos):
        raise NotImplementedError()



""" OLD Implementation
def compute_cea(table, candidates, store, cd, resources, entropy):
    results = []
    
    for row_idx, table_row in enumerate(table[1]):
        row = rm.Row()
        for pos, cell in enumerate(table_row.values()):
            norm = Normalizer([]).normalize_cell(cell)  # TODO: BAD api
            
            cands = [
                f"http://dbpedia.org/resource/{uri}"
                for uri in candidates.get(norm, [])
            ]
                
            if cd.is_necol(table[0], pos, row_idx + 1):
                row.add_ne_cell(cell, norm, cands, is_subject=cd.is_subjectcol(table[0], pos, row_idx + 1))
            else:
                row.add_lit_cell(cell, cell, cands) # TODO
                
        # TODO: if subject col is not in column 0
        if row.get_subject_cell() is None:
            print("WARNING: subject col is not 0. Ignoring...")
            continue
                
        table_rm = rm.RowMatcher(row, store)

        subjects, linkages = table_rm.compute_linked_entities(resources, entropy)
        results.append((subjects, linkages))
            
    return results 
"""