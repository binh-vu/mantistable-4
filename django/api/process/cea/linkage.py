import collections
import sys
import datatype

from itertools import groupby

from api.process.normalization import cleaner
from api.process.cea.models.cell import Cell
from api.process.cea.models.link import Link
from api.process.cea.models.row import Row
from api.process.utils.lamapi import LamAPIWrapper
from api.process.utils.math_utils import edit_distance, step

import mantistable.settings


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
            s_tmp = [
                l.subject()
                for l in link
            ]

            l_subj = {
                k: step(v)
                for k, v in collections.Counter(s_tmp).items()
            }

            subjects = {
                k: subjects.get(k, 0) + l_subj.get(k, 0)
                for k in set(subjects) | set(l_subj)
            }

        # Subjects confidence
        for k, v in subjects.items():
            confidence = self._get_candidate_confidence(k, subj_cell)
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
            confidence = self._get_candidate_confidence(o, cell2)
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
        # TODO: Filter by xsd not datatype, also should I use column analysis xsd datatype??
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

        print(links_same_datatype)

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
            
            # TODO: Implement single-dispatch pattern
            # Compute the confidence score
            if xsd_datatype.label() == "xsd:float":
                # confidence = (value - upper) / (lower - upper)
                dummy_subject = candidates_value[0][0]
                comparation_line = [(dummy_subject, "dummy_predicate", sys.float_info.min)] + \
                                   [cv for cv in candidates_value] + \
                                   [(candidates_value[-1][0], candidates_value[-1][1], sys.float_info.max)]

                for idx in range(0, len(comparation_line) - 1):
                    lower_triple = comparation_line[idx]
                    upper_triple = comparation_line[idx + 1]

                    lower_value = lower_triple[2]
                    upper_value = upper_triple[2]
                    
                    if lower_value == upper_value and cell_python_value == lower_value:
                        res.append(Link(triple=lower_triple, confidence=1.0))
                    elif cell_python_value >= lower_value and cell_python_value < upper_value:
                        confidence_lower = (cell_python_value - upper_value) / (lower_value - upper_value)
                        confidence_lower = min(max(confidence_lower, 0.0), 1.0)
                        confidence_upper = 1.0 - confidence_lower
                        if lower_triple[1] != "dummy_predicate":
                            # res.append(Link(triple=lower_triple, confidence=confidence_lower))    
                            # TODO: I don't use membership function confidence anymore for a problem in the revision,
                            #       but this new score is technically bugged...
                            conf = 1.0 - (abs(cell_python_value - lower_value) / max(cell_python_value, lower_value, 1.0))
                            res.append(Link(triple=lower_triple, confidence=conf))

                        if idx + 1 < len(comparation_line) - 1 and confidence_upper > 0.0:
                            #res.append(Link(triple=comparation_line[idx + 1], confidence=confidence_upper))
                            conf = 1.0 - (abs(cell_python_value - upper_value) / max(cell_python_value, upper_value, 1.0))
                            res.append(Link(triple=upper_triple, confidence=conf))
                        
                        break
                    
            elif xsd_datatype.label() == "xsd:date":
                for cv in candidates_value:
                    if cv[2] == cell_python_value:
                        res.append(Link(triple=cv, confidence=1.0))
            else:
                cell_python_value = cell2.content
                for cv in candidates_value:
                    confidence = 1.0 - edit_distance(cv[2], cell_python_value)
                    if confidence > 0.0:
                        res.append(Link(triple=cv, confidence=confidence))

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

    def _get_candidate_confidence(self, candidate, cell):
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

        return confidence
