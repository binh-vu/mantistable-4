from api.process.utils.lamapi.wrapper import LamAPIWrapper
from api.process.cea.models.cell import Cell
import datatype

class LinksRetrieval:
    def __init__(self, pairs, lamapi_backend):
        self._pairs = pairs
        self.lamapi = LamAPIWrapper(
            lamapi_backend["host"],
            lamapi_backend["port"],
            lamapi_backend["accessToken"]
        )

    def get_links(self):
        linkages = {}
        for subj, obj in self._pairs:
            subj_cell = Cell(*subj)
            obj_cell = Cell(*obj)
            if not obj_cell.is_lit_cell:
                links = self.get_ne_links(subj_cell, obj_cell)
            else:
                links = self.get_lit_links(subj_cell, obj_cell)

            linkages.update(links)

        return linkages

    def get_ne_links(self, subj_cell, obj_cell):
        links = {}
        cand_subjects = {}
        cand_objects = set()

        # Get candidates objects from lamapi service
        cand_lamapi_objects, cand_lamapi_predicates = self._get_candidates_objects(subj_cell.candidates_entities())
        for cand1_uri, candidates_objects in cand_lamapi_objects.items():
            for sub_obj in candidates_objects:
                cand_objects.add(sub_obj)
                if sub_obj not in cand_subjects:
                    cand_subjects[sub_obj] = []
                    
                cand_subjects[sub_obj].append(cand1_uri)
        
        # Intersection between subject candidates objects and object's candidates
        triples = []
        for candidate_obj in cand_objects.intersection(set(obj_cell.candidates_entities())):
            for candidate_subj in cand_subjects[candidate_obj]:
                p = cand_lamapi_predicates.get((candidate_subj, candidate_obj), None)
                if p is not None:
                    triple = (candidate_subj, p, candidate_obj)
                    triples.append(triple)

        links[(subj_cell.content, obj_cell.content)] = triples
        return links

    def get_lit_links(self, subj_cell, obj_cell):
        links = {
            (subj_cell.content, obj_cell.content): [
                (s, p, o)
                for s, p, o in self._get_candidates_literals(subj_cell.candidates_entities())
            ]
        }
        
        # Filter candidates by datatype
        datatype_cell = datatype.get_datatype(obj_cell.normalized)
        xsd_datatype = datatype_cell.get_xsd_type()

        """
        links_same_datatype = sorted(
            [
                triple
                for triple in links
                if datatype.get_datatype(triple[3]).get_xsd_type().label() == xsd_datatype.label()
            ],
            key=lambda item: item.subject()
        )
        """
        links_same_datatype = {
            pair: [
                triple
                for triple in triples
                if datatype.get_datatype(triple[2]).get_xsd_type().label() == xsd_datatype.label()
            ]
            for pair, triples in links.items()
        }

        return links_same_datatype

    def _get_candidates_objects(self, candidates):
        """
            Get objects from Lamapi service
        """
        cand_lamapi_objects = {}
        cand_lamapi_predicates = {}
        for candidate, po in self.lamapi.objects(candidates).items():
            cand_lamapi_objects[candidate] = []
            for pred, objs in po.items():
                cand_lamapi_objects[candidate].extend(objs)

                for obj in objs:
                    cand_lamapi_predicates[(candidate, obj)] = pred
            
            cand_lamapi_objects[candidate] = set(cand_lamapi_objects[candidate])


        return cand_lamapi_objects, cand_lamapi_predicates

    def _get_candidates_literals(self, candidates):
        """
            Get literal triples from Lamapi service
        """
        cand_lamapi_triples = []

        for s, pl in self.lamapi.literals(candidates).items():
            for p, lits in pl.items():
                cand_lamapi_triples.extend([
                    (s, p, l)
                    for l in lits
                ])

        return list(set(cand_lamapi_triples))

    def get_pairs(self):
        return self._pairs