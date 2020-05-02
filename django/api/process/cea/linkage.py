import sys
import collections
import editdistance
from itertools import groupby

import normalize.cleaner as cleaner
from repository.solr import is_person

from api.process.cea.models.cell import Cell
from api.process.cea.models.row import Row
from api.process.cea.models.link import Link
from api.process.utils.lamapi import LamAPIWrapper

import mantistable.settings
    
# Edit distance
def edit_distance(s1, s2):
    return editdistance.eval(s1, s2) / max((len(s1), len(s2), 1))

# Step function
def step(x):
    return 1 * (x > 0)

# TODO: Refactor this
def convert_to(value, xsd):    
    if xsd == "numeric":
        val = float(value)
        if val == float("inf"):
            val = sys.float_info.max
        elif val == float("-inf"):
            val = sys.float_info.min
            
        return val
    elif xsd == "date": # TODO: Too simple conversion...        
        if "/" in value:
            return value.replace("/", "-")
        else:
            return value
    
    return value


class Linkage:
    def __init__(self, row):
        self.row = row
        self.lamapi = LamAPIWrapper(mantistable.settings.LAMAPI_HOST, mantistable.settings.LAMAPI_PORT)

    def get_links(self):
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
        subj_cell = self.row.get_subject_cell()

        subjects = {}
        for link in links:
            s_tmp = []
            for l in link:
                s_tmp.append(l.subject())

            l_subj = {
                k: step(v)
                for k, v in collections.Counter(s_tmp).items()
            }

            subjects = {
                k: subjects.get(k, 0) + l_subj.get(k, 0)
                for k in set(subjects) | set(l_subj)
            }

        # subjects confidence
        for k, v in subjects.items():            
            confidence = self._get_candidate_confidence(k, subj_cell)
            subjects[k] = confidence * (v / len(links))
            
        return subjects

    def _match_ne_cells(self, subj_cell, cell2):        
        links = []

        cand_objects = set()
        os_map = {}
        for cand1_uri in subj_cell.candidates:
            for sub_obj in set(self.lamapi.objects([cand1_uri]).values()):   # TODO: change algorithm to query lists, also check algorithm: literals or ne?
                cand_objects.add(sub_obj)
                if sub_obj not in os_map:
                    os_map[sub_obj] = []
                    
                os_map[sub_obj].append(cand1_uri)
        
        for co in cand_objects.intersection(set(cell2.candidates)):
            for uri in os_map[co]:
                for s, p, o in self._match((uri, None, co)):         
                    confidence = self._get_candidate_confidence(o, cell2)
                    #confidence = labels_confidence[o]
                    
                    links.append( Link(triple=(s, p, o), confidence=confidence) )
                    
        return links

    def _match_lit_cells(self, cell1, cell2):
        links = []
        
        cand_objects = set()
        os_map = {}
        for cand1 in cell1.candidates:
            for sub_obj in set(self.lamapi.objects([cand1]).values()):  # TODO: change algorithm to query lists
                # TODO: With the redis implementation this is different: lamapi.objects return always literals
                # TODO: Also redis implemetation have dbo and foaf
                if not sub_obj.startswith("http://dbpedia.org/resource/"):  # TODO: This is a simple way to discriminate between entities and literals
                    cand_objects.add(sub_obj)
                    if sub_obj not in os_map:
                        os_map[sub_obj] = []
                        
                    os_map[sub_obj].append(cand1)
                
        for co in cand_objects:
            for cand1 in os_map[co]:
                for s, p, o in self._match((cand1, None, co)):
                    links.append( Link(triple=(s, p, o), confidence=0.0) )
                    
        xsd_cell = get_xsd(cell2.normalized)
        cell_value = convert_to(cell2.normalized, xsd_cell)
        all_cells = sorted(
            list(filter(
                lambda item: xsd_cell == get_xsd(item.object()), links)
            ), 
            key=lambda item: item.subject()
        )
        
        groups = [] 
        for _, group in groupby(all_cells, lambda item: item.subject()): 
            groups.append(list(group))
        
        res = []
        for group in groups:
            # Convert to same xsd
            candidates_value = [
                (
                    linkage.subject(),
                    linkage.predicate(),
                    convert_to(linkage.object(), xsd_cell)
                )
                for linkage in group
            ]
            candidates_value.sort(key=lambda item: item[2])
            
            if len(candidates_value) == 0:
                return []
            
            # Compute the confidence score
            if xsd_cell == "numeric":
                # confidence = (value - upper) / (lower - upper)
                min_float = sys.float_info.min
                max_float = sys.float_info.max
                dummy_s, _, __ = candidates_value[0]
                tmp = [(dummy_s, "dummy", min_float)] + [cv for cv in candidates_value] + [(candidates_value[-1][0], candidates_value[-1][1], max_float)]

                for idx in range(0, len(tmp) - 1):
                    lower = tmp[idx][2]
                    upper = tmp[idx + 1][2]
                    
                    if lower == upper and cell_value == lower:
                        res.append(Link(triple=tmp[idx], confidence=1.0))
                    elif cell_value >= lower and cell_value < upper:
                        confidence_lower = (cell_value - upper) / (lower - upper)
                        confidence_lower = min(max(confidence_lower, 0.0), 1.0)
                        confidence_upper = 1.0 - confidence_lower
                        if tmp[idx][1] != "dummy":
                            # res.append(Link(triple=tmp[idx], confidence=confidence_lower))    
                            # TODO: I don't use membership function confidence anymore for a problem in the revision,
                            #       but this new score is technically bugged...
                            conf = 1.0 - (abs(cell_value - tmp[idx][2]) / max(cell_value, tmp[idx][2], 1.0))
                            res.append(Link(triple=tmp[idx], confidence=conf))

                        if idx + 1 < len(tmp) - 1 and confidence_upper > 0.0:
                            #res.append(Link(triple=tmp[idx + 1], confidence=confidence_upper)) # TODO
                            conf = 1.0 - (abs(cell_value - tmp[idx + 1][2]) / max(cell_value, tmp[idx + 1][2], 1.0))
                            res.append(Link(triple=tmp[idx + 1], confidence=conf))
                        break
                    
            elif xsd_cell == "date":
                for cv in candidates_value:
                    if cv[2] == cell_value:
                        res.append(Link(triple=cv, confidence=1.0))
            else:
                cell_value = cell2.content
                for cv in candidates_value:
                    ed = edit_distance(cv[2], cell_value)
                    
                    """ TODO: Jaccard for long strings should be used
                    cell_toks  = set(cell_value.replace("-", " ").split(" "))
                    cv_toks = (cv[2].replace("-", " ").split(" "))
                    
                    jaccard = len(cell_toks.intersection(cv_toks)) / max(len(cell_toks), len(cv_toks), 1)
                    """
                    
                    confidence = 1.0 - ed
                    if confidence > 0.0:
                        #confidence *= 1 #entropy[cv[2]] / max_bits  # TODO
                        res.append(Link(triple=cv, confidence=confidence))

        return res

    # TODO: Change api signature _match(s, o) -> [(s, p, o)]
    def _match(self, triple):
        s, p, o = triple

        results = []
        predicates = self.lamapi.predicates([" ".join([s, o])])
        for p in predicates:
            results.append(
                (s, p, o)
            )

        return results

    def _get_candidate_confidence(self, candidate, cell):
        """
            Compute the max confidence of a given candidate by matching candidate's labels with cell content
        """
        # TODO: What about redirections?
        # TODO: I need a map <entity> -> [<norm_label>, <norm_label>,...]
        #labels = resources.get(candidate[28:], [])
        norm_labels = []
        
        # Replace this piece of code with the resources map (label is normalized label)
        if is_person(cell.content):
            tokens = candidate[28:].split("_")
            if len(tokens[0]) > 0:
                tokens[0] = tokens[0][0]

            label = " ".join(tokens).lower()          
        else:
            label = candidate[28:].lower().replace("_", " ")
            
        norm_labels.append(label)

        winning_conf = 0.0
        for norm_label in norm_labels:
            confidence = 1.0 - edit_distance(cell.normalized, norm_label)
            
            if confidence > winning_conf:
                winning_conf = confidence

        return confidence
        #1.0 - edit_distance(cell.normalized, nor_label)

# ------------------------------------------------------------------------------
# TODO: Bad utility function that must be replaced with phase 2 datatype
#       extraction

"""
# DEPRECATED
def get_xsd(value):
    if isnumeric(value):
        return "numeric"
    elif isdate(value):
        return "date"
        
    return "string"

# DEPRECATED
def isnumeric(value):
    try:
        float(value)
        return True
    except:
        return False

# DEPRECATED
def isinteger(value):
    try:
        int(value)
        return True
    except:
        return False

# DEPRECATED
def isdate(value):  # TODO: Too simple algorithm...
    def is_hms(value, split_chr):
        hms = value.split(split_chr)
        if len(hms) != 3:
            return False
        
        for p in hms:
            if not isinteger(p):
                return False
            
        return True
    
    return is_hms(value, "/") or is_hms(value, "-")
"""