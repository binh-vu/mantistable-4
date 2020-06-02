import collections

class CPAProcess:
    def __init__(self, cea):
        self._cea = cea

    def compute(self):
        table_triples = []

        for _, (_, linkages) in enumerate(self._cea):        
            triples = [
                [
                    (link.get_triple(), link.get_confidence())
                    for link in linkage
                ]
                for linkage in linkages
            ]
            table_triples.append(triples)
        
        predicates = []        
        if len(table_triples) > 0:
            columns_count = len(table_triples[0])
            
            for _ in range(0, columns_count):
                predicates.append({})
            
            for triples in table_triples:
                for col_idx, links in enumerate(triples):
                    link_preds = {}
                    for link in links:
                        triple, confidence = link
                        
                        predicate = triple[1]
                        
                        if predicate not in link_preds:
                            link_preds[predicate] = 0.0
                        
                        # TODO: remove this
                        link_preds[predicate] = max(link_preds[predicate], confidence)

                    for predicate in link_preds:
                        if predicate not in predicates[col_idx]:
                            predicates[col_idx][predicate] = 0.0
                        
                        predicates[col_idx][predicate] += 1
        
        if len(self._cea) != 0:
            for col_idx, d in enumerate(predicates):
                norm_factor = len(self._cea)
                if norm_factor != 0.0:
                    for pred in d:
                        predicates[col_idx][pred] /= norm_factor
                        
        return predicates