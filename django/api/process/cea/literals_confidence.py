from functools import singledispatch
from decimal import Decimal
from datatype import xsd, validators

import sys
import math

from api.process.cea.models.link import Link
from api.process.utils.math_utils import edit_distance

# TODO: Extract function to utils.math_utils
# TODO: I don't use membership function confidence anymore for a problem in the revision,
#       but this new score is technically bugged...
# TODO: This bug is shown in the following example:
#       1.0 - (abs(-2 - 5) / max(-2, 5, 1)) = -0.39999
#       but confidence should always be between 0.0 and 1.0
# PROPOSAL: Score based on a modification of gaussian probability density function without the normalization factor.
#           In this way confidence is 1.0 when the distance between values is zero
#           and we are able to adjust the spread by editing the standard deviation
#           e^-0.5*((a - b)/σ)^2
#           Standard deviation could be adjusted to reflect the column distribution.
def numeric_confidence(a, b):
    #return 1.0 - (abs(a - b) / max(a, b, 1.0))
    try:
        sigma = Decimal(100) # TODO: adjust this to reflect column distribution??
        return Decimal(math.e)**( Decimal(-0.5)*((Decimal(a) - Decimal(b)) / sigma)**Decimal(2))
    except:
        return 0.0

def literal_exact_match(cell_value, candidates_value: list):
    links = []
    for cv in candidates_value:
        if cv[2] == cell_value:
            s, p, o = tuple(map(lambda item: str(item), cv))

            links.append( Link(triple=(s, p, o), confidence=1.0) )

    return links

def literal_editdistance_match(cell_value: str, candidates_value: list):
    links = []
    for cv in candidates_value:
        confidence = 1.0 - edit_distance(cv[2], cell_value)
        if confidence > 0.0:
            links.append( Link(triple=cv, confidence=confidence) )

    return links

def literal_numeric_match(cell_value: float, candidates_value: list):
    links = []

    dummy_subject = candidates_value[0][0]
    comparation_line = [(dummy_subject, "dummy_predicate", sys.float_info.min)] + \
                       [cv for cv in candidates_value] + \
                       [(candidates_value[-1][0], candidates_value[-1][1], sys.float_info.max)]

    for idx in range(0, len(comparation_line) - 1):
        lower_triple = comparation_line[idx]
        upper_triple = comparation_line[idx + 1]

        lower_value = lower_triple[2]
        upper_value = upper_triple[2]
        
        try:
            if lower_value == upper_value and cell_value == lower_value:
                links.append(Link(triple=(lower_triple[0], lower_triple[1], str(lower_triple[2])), confidence=1.0))
            elif cell_value >= lower_value and cell_value < upper_value:
                confidence_lower = float((Decimal(cell_value) - Decimal(upper_value)) / (Decimal(lower_value) - Decimal(upper_value)))
                confidence_lower = min(max(confidence_lower, 0.0), 1.0)
                confidence_upper = 1.0 - confidence_lower

                if lower_triple[1] != "dummy_predicate":
                    conf = float(numeric_confidence(Decimal(cell_value), Decimal(lower_value)))
                    if conf > 0.0001:
                        links.append( Link(triple=(lower_triple[0], lower_triple[1], str(lower_triple[2])), confidence=conf) )

                if idx + 1 < len(comparation_line) - 1 and confidence_upper > 0.0:
                    conf = float(numeric_confidence(Decimal(cell_value), Decimal(upper_value)))
                    if conf > 0.0001:
                        links.append( Link(triple=(upper_triple[0], upper_triple[1], str(upper_triple[2])), confidence=conf) )
                
                break
        except:
            pass

    return links

@singledispatch
def literal_confidence(xsd_type, cell_value, candidates_value: list):
    # TODO: Should I use a fallback implementation like edit distance or something alike
    raise NotImplementedError()

@literal_confidence.register
def literal_numeric_confidence(xsd_type: xsd.numeric.XsdNumeric, cell_value, candidates_value: list):
    return literal_numeric_match(cell_value, candidates_value)

@literal_confidence.register
def literal_date_confidence(xsd_type: xsd.date.XsdDate, cell_value, candidates_value: list):
    # TODO: Should use a distance function or something alike not exact matching
    return literal_exact_match(cell_value, candidates_value)

@literal_confidence.register
def literal_string_confidence(xsd_type: xsd.string.XsdString, cell_value, candidates_value: list):
    return literal_editdistance_match(cell_value, candidates_value)

@literal_confidence.register
def literal_boolean_confidence(xsd_type: xsd.boolean.XsdBoolean, cell_value, candidates_value: list):
    return literal_exact_match(cell_value, candidates_value)

@literal_confidence.register
def literal_url_confidence(xsd_type: xsd.anyURI.XsdUri, cell_value, candidates_value: list):
    return literal_editdistance_match(cell_value, candidates_value)

@literal_confidence.register
def literal_geocoord_confidence(xsd_type: xsd.geo.XsdGeo, cell_value: validators.geocoord.Point, candidates_value: list):
    lat_candidates_value = [
        o.latitude()
        for s, p, o in candidates_value
    ]
    lng_candidates_value = [
        o.longitude()
        for s, p, o in candidates_value
    ]
    
    lat_conf = literal_numeric_match(cell_value.latitude(), lat_candidates_value)
    lng_conf = literal_numeric_match(cell_value.longitude(), lng_candidates_value)

    assert len(lat_conf) == len(lng_conf)

    return [
        Link(
            triple=lat_conf[i].get_triple(),            
            confidence=lat_conf[i].get_confidence() * lng_conf[i].get_confidence()
        )
        for i in range(0, len(lat_conf))
    ]
