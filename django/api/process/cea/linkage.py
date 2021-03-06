import collections
import sys
import datatype
import functools

from itertools import groupby

from api.process.cea.models.cell import Cell
from api.process.cea.models.link import Link
from api.process.cea.models.row import Row
from api.process.utils.rules import person_rule as rules
from api.process.utils.lamapi import LamAPIWrapper
from api.process.utils.math_utils import edit_distance, step

import api.process.cea.literals_confidence as lit_utils
import mantistable.settings


class Linkage:
    def __init__(self, row, lamapi_backend):
        self.row = row
        #self.triples = triples
        self.lamapi = LamAPIWrapper(
            lamapi_backend["host"],
            lamapi_backend["port"],
            lamapi_backend["accessToken"],
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
                if cell.is_lit_cell:
                    link = self._match_lit_cells(subj_cell, cell)
                else:
                    link = self._match_ne_cells(subj_cell, cell)
                
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
            confidence = self._get_candidate_confidence(k, subj_cell)
            subjects[k] = confidence * (v / len(links))    # TODO: <<<<<<<<<<< Check the algorithm
            
        return subjects

    def _match_ne_cells(self, subj_cell, cell2):
        """
            Get triples between Subject and Named Entity cell
        """       
        links = []
        
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
        for candidate_obj in cand_objects.intersection(set(cell2.candidates_entities())):
            for candidate_subj in cand_subjects[candidate_obj]:
                preds_set = cand_lamapi_predicates.get((candidate_subj, candidate_obj), set())
                for p in preds_set:
                    confidence = self._get_candidate_confidence(candidate_obj, cell2)
                    links.append( Link(triple=(candidate_subj, p, candidate_obj), confidence=confidence) )

        """
        for triple in self.triples.get(str(hash((subj_cell.content, cell2.content))), []):
            confidence = self._get_candidate_confidence(triple[2], cell2)
            links.append( Link(triple=tuple(triple), confidence=confidence) )
        """

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

        if xsd_datatype.label() == "xsd:date":
            possible_dates = []
            
            for link in links:
                if datatype.get_datatype(link.object()).get_xsd_type().label() == "xsd:float":
                    obj = datatype.get_datatype(link.object()).to_python()
                    
                    try:
                        int(obj)                            
                    except:
                        break
                    
                    if datatype.get_datatype(f"{int(obj)}-01-01").get_xsd_type().label() == "xsd:date":
                        possible_dates.append(
                            Link(
                                triple=(link.triple[0], link.triple[1], f"{int(obj)}-01-01"),
                                confidence=link.get_confidence()
                            )
                        )
            
            
            """
            links_same_datatype.extend(
                [
                    Link(triple=(link.triple[0], link.triple[1], f"{int(datatype.get_datatype(link.object()).to_python())}-01-01"), confidence=link.get_confidence())
                    for link in links
                    if datatype.get_datatype(link.object()).get_xsd_type().label() == "xsd:float"
                    if datatype.get_datatype(f"{int(datatype.get_datatype(link.object()).to_python())}-01-01").get_xsd_type().label() == "xsd:date"
                ]
            )
            """
                
            links_same_datatype.extend(possible_dates)
            links_same_datatype = sorted(links_same_datatype, key=lambda item: item.subject())


        #--------------

        """
        datatype_cell = datatype.get_datatype(cell2.normalized)
        xsd_datatype = datatype_cell.get_xsd_type()
        cell_python_value = datatype_cell.to_python()
        links_same_datatype = self.triples.get(str(hash((cell1.content, cell2.content))), [])

        links_same_datatype = sorted(
            [
                Link(triple=link, confidence=0.0)
                for link in links_same_datatype
            ],
            key=lambda item: item.subject()
        )
        """

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
            triples = lit_utils.literal_confidence(xsd_datatype, cell_python_value, candidates_value)
            res.extend(triples)

        return res

    """
    def _match(self, subj_object_pairs: list):
        #
            Match subject object pairs to find predicates from Lamapi service
        #
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
    """

    def _get_candidates_objects(self, candidates):
        #Get objects from Lamapi service
        cand_lamapi_objects = {}
        cand_lamapi_predicates = {}
        try:
            for candidate, po in self.lamapi.objects(candidates).items():
                cand_lamapi_objects[candidate] = []
                for pred, objs in po.items():
                    cand_lamapi_objects[candidate].extend(objs)

                    for obj in objs:
                        if (candidate, obj) not in cand_lamapi_predicates:
                            cand_lamapi_predicates[(candidate, obj)] = set()
                        
                        cand_lamapi_predicates[(candidate, obj)].add(pred)
                
                cand_lamapi_objects[candidate] = set(cand_lamapi_objects[candidate])
        except Exception as e:
            print("[api.process.cea.linkage] Error", candidates)
            raise


        return cand_lamapi_objects, cand_lamapi_predicates

    def _get_candidates_literals(self, candidates):
        #Get literal triples from Lamapi service
        cand_lamapi_triples = []
        buffer = set(candidates)

        for s, pl in self.lamapi.literals(list(buffer)).items():
            for p, lits in pl.items():
                cand_lamapi_triples.extend([
                    (s, p, l)
                    for l in lits
                    if not isinstance(l, dict)  # TODO: HACK! Remove it
                ])

        for c in cand_lamapi_triples:
            s, p, l = c
            if isinstance(l, dict):
                print("l", s, p, l)
        return list(set(cand_lamapi_triples))

    def _get_candidate_confidence(self, candidate, cell):
        """
            Compute the max confidence of a given candidate by matching candidate's labels with cell content
        """

        """ TODO
        rule = rules.PersonRule(cell.content)
        if rule.match():
            candidate_label = rule.build_label(cell.normalized)
        else:
            candidate_label = cell.normalized
        """
        candidate_label = cell.normalized

        #key = (candidate, candidate_label)

        winning_conf = 0.0
        for normalized_label in cell.candidates_labels(candidate):
            confidence = 1.0 - edit_distance(candidate_label, normalized_label)
            
            if confidence > winning_conf:
                winning_conf = confidence

        return winning_conf
