import collections
import sys
import datatype
import functools

from itertools import groupby

from api.process.normalization import cleaner
from api.process.cea.models.cell import Cell
from api.process.cea.models.link import Link
from api.process.cea.models.row import Row
import api.process.cea.literals_confidence as lit_utils
from api.process.utils.lamapi import LamAPIWrapper
from api.process.utils.math_utils import edit_distance, step

import mantistable.settings

import celery.utils.functional as functional

@functional.memoize(maxsize=None)
def _get_candidate_confidence(candidate, cell):
    """
        Compute the max confidence of a given candidate by matching candidate's labels with cell content
    """
    """ TODO Move this code outside this function (cell candidates should have it)
    if is_person(cell.content):
        tokens = candidate[28:].split("_")
        if len(tokens[0]) > 0:
            tokens[0] = tokens[0][0]

        label = " ".join(tokens).lower()
    """

    winning_conf = 0.0
    for normalized_label in cell.candidates_labels(candidate):
        confidence = 1.0 - edit_distance(cell.normalized, normalized_label)
        
        if confidence > winning_conf:
            winning_conf = confidence

    return winning_conf

class Linkage:
    def __init__(self, row):
        self.row = row
        self.lamapi = LamAPIWrapper(
            mantistable.settings.LAMAPI_HOST,
            mantistable.settings.LAMAPI_PORT
        )

    def get_links(self):
        """
            Get links (triples and confidence) of row
        """
        subj_cell = self.row.get_subject_cell()
        cells = self.row.get_cells()

        links = []
        for cell in cells:
            if cell != subj_cell:
                if not cell.is_lit_cell:
                    link = self._match_ne_cells(subj_cell, cell)
                else:
                    link = self._match_lit_cells(subj_cell, cell)
                
                links.append(link)

        return links
        
    def get_subjects(self, links):
        """
            Get candidate subjects with confidence
        """
        subj_cell = self.row.get_subject_cell()

        subjects = {}
        for link in links:
            subjects_buffer = [
                l.subject()
                for l in link
            ]

            l_subj = {
                k: step(v)
                for k, v in collections.Counter(subjects_buffer).items()
            }

            subjects = {
                k: subjects.get(k, 0) + l_subj.get(k, 0)
                for k in set(subjects) | set(l_subj)
            }

        # Subjects confidence
        for k, v in subjects.items():
            confidence = _get_candidate_confidence(k, subj_cell)
            subjects[k] = confidence * (v / len(links))
            
        return subjects

    def _match_ne_cells(self, subj_cell, cell2):
        """
            Get triples between Subject and Named Entity cell
        """       
        links = []
        
        cand_subjects = {}
        cand_objects = set()

        # Get candidates objects from lamapi service
        cand_lamapi_objects = self._get_candidates_objects(subj_cell.candidates_entities())
        for cand1_uri, candidates_objects in cand_lamapi_objects.items():
            for sub_obj in candidates_objects:
                cand_objects.add(sub_obj)
                if sub_obj not in cand_subjects:
                    cand_subjects[sub_obj] = []
                    
                cand_subjects[sub_obj].append(cand1_uri)
        
        # Intersection between subject candidates objects and object's candidates
        candidates_pair = []
        for candidate_obj in cand_objects.intersection(set(cell2.candidates_entities())):
            for candidate_subj in cand_subjects[candidate_obj]:
                candidates_pair.append((candidate_subj, candidate_obj))

        # TODO: Check if I can reuse predicates from lamapi objects endpoint
        for s, p, o in self._match(candidates_pair):
            confidence = _get_candidate_confidence(o, cell2)
            links.append( Link(triple=(s, p, o), confidence=confidence) )

        # Calculate max confidence for the same triple (different labels)
        max_conf_links = {}
        for link in set(links):
            if link.triple not in max_conf_links:
                max_conf_links[link.triple] = link.confidence

            if max_conf_links[link.triple] < link.confidence:
                max_conf_links[link.triple] = link.confidence

        links = [
            Link(triple=triple, confidence=confidence)
            for triple, confidence in max_conf_links.items()
        ]
                    
        return links

    def _match_lit_cells(self, cell1, cell2):
        """
            Get triples between Subject and Literal cell
        """
        links = [
            Link(triple=triple, confidence=0.0)
            for triple in self._get_candidates_literals(cell1.candidates_entities())
        ]
        
        # Filter candidates by datatype
        datatype_cell = datatype.get_datatype(cell2.normalized)
        xsd_datatype = datatype_cell.get_xsd_type()
        cell_python_value = datatype_cell.to_python()

        links_same_datatype = sorted(
            [
                link
                for link in links
                if datatype.get_datatype(link.object()).get_xsd_type().label() == xsd_datatype.label()
            ],
            key=lambda item: item.subject()
        )

        # Group links by subject
        groups = [
            list(group)
            for _, group in groupby(
                links_same_datatype,
                lambda item: item.subject()
            )
        ]

        res = []
        for group in groups:
            # Convert to same xsd
            candidates_value = [
                (
                    linkage.subject(),
                    linkage.predicate(),
                    datatype.get_datatype(linkage.object()).to_python()
                )
                for linkage in group
            ]
            candidates_value.sort(key=lambda item: item[2])
            
            if len(candidates_value) == 0:
                return []
            
            # Compute the confidence score
            res.extend(lit_utils.literal_confidence(xsd_datatype, cell_python_value, candidates_value))

        return res

    def _match(self, subj_object_pairs: list):
        """
            Match subject object pairs to find predicates from Lamapi service
        """
        pairs = [
            " ".join(pair)
            for pair in subj_object_pairs
        ]

        triples = []
        lamapi_results = self.lamapi.predicates(pairs)
        for pair, preds in lamapi_results.items():
            s, o = tuple(pair.split(" "))
            for p in preds:
                triples.append(
                    (s, p, o)
                )

        return triples

    def _get_candidates_objects(self, candidates):
        """
            Get objects from Lamapi service
        """
        cand_lamapi_objects = {}
        for candidate, po in self.lamapi.objects(candidates).items():
            cand_lamapi_objects[candidate] = []
            for objs in po.values():
                cand_lamapi_objects[candidate].extend(objs)
            
            cand_lamapi_objects[candidate] = set(cand_lamapi_objects[candidate])

        return cand_lamapi_objects

    def _get_candidates_literals(self, candidates):
        """
            Get literal triples from Lamapi service
        """
        triples = []
        for s, pl in self.lamapi.literals(candidates).items():
            for p, l in pl.items():
                triples.append(
                    (s, p, l)
                )

        return list(set(triples))
