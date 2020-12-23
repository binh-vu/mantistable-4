import copy
import enum
import os
import pickle
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import List, Dict, Optional, Set, Union

import rdflib
from rdflib import Graph, URIRef, OWL, RDF, RDFS, XSD, Literal, Namespace

from sm_unk.misc import serialize_pkl, deserialize_pkl


class OntPredicateType(enum.Enum):
    Unspecified = "unspecified"
    DataProperty = "data_property"
    ObjectProperty = "object_property"


@dataclass
class OntClass:
    uri: str
    # none if the class does not have label
    label: Optional[str]
    subClassOf: List['Class']
    parents: Set[str] = field(default_factory=set)
    # no definition if this class is a class created by linking to another class in other ontology
    # for example: from link in rdf:subClassOf, or rdfs:range
    no_def: bool = False


@dataclass
class OntPredicate:
    uri: str
    label: str
    domains: List[OntClass]
    ranges: List[OntClass]
    type: OntPredicateType


class Ontology:
    default_prefixes = {
        "dbpedia-owl": "http://dbpedia.org/ontology/",
        "dbpedia-resource": "http://dbpedia.org/resource/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "schema": "http://schema.org/"
    }

    def __init__(self, classes: Dict[str, OntClass], predicates: Dict[str, OntPredicate],
                 prefixes: Dict[str, str]=None, infiles: List[str]=None):
        self.classes = classes
        self.predicates = predicates
        self.prefixes = copy.copy(self.default_prefixes)
        # list of file paths indicates where the ontology is loaded from
        self.infiles = infiles or []
        if prefixes is not None:
            self.prefixes.update(prefixes)

    @classmethod
    def empty(cls):
        return cls({}, {})

    @classmethod
    def from_files(cls, infiles: List[str], prefixes: dict=None, cache: bool=True):
        ont = Ontology.empty()
        for infile in infiles:
            ont.merge(cls.from_file(infile, prefixes, cache))
        ont.prefixes.update(prefixes)
        return ont

    @classmethod
    def from_file(cls, infile, prefixes: dict=None, cache: bool=True):
        infile_path = Path(os.path.abspath(infile))
        cache_file = infile_path.parent / f"{infile_path.stem}.pkl"
        if cache_file.exists():
            classes, predicates, prefixes = deserialize_pkl(cache_file)
            return cls(classes, predicates, prefixes)
        else:
            instance = cls._from_file(infile_path, prefixes)
            if cache:
                serialize_pkl((instance.classes, instance.predicates, instance.prefixes), cache_file)
            return instance

    @classmethod
    def _from_file(cls, infile: Path, prefixes: dict=None):
        def make_dict(pairs):
            odict = {}
            for k, v in pairs:
                if k not in odict:
                    odict[k] = []
                odict[k].append(v)
            return odict

        if infile.name.endswith("ttl"):
            format = "ttl"
        else:
            raise NotImplementedError()

        g = Graph()
        g.parse(str(infile), format=format)
        g.namespace_manager.bind("owl", OWL)

        root_classes = {OWL.Thing, URIRef("http://schema.org/Thing")}
        classes = {}
        accepted_languages = {None, "en"}

        # load classes
        resp = g.query("""
            select ?s where {
                { ?s rdf:type rdfs:Class } UNION { ?s rdf:type owl:Class }.
            }""")
        for s, in resp:
            uri = str(s)
            cdict = make_dict(g.predicate_objects(s))
            if RDFS.label not in cdict:
                label = None
            else:
                labels = [x for x in cdict[RDFS.label] if x.language in accepted_languages]
                if len(labels) == 0:
                    label = None
                else:
                    assert len(labels) == 1, labels
                    label = labels[0].value
            subclassof = [str(x) for x in cdict.get(RDFS.subClassOf, []) if x not in root_classes]
            classes[uri] = OntClass(uri, label, subclassof)

        # load predicates
        predicates = {}
        resp = g.query("""
                    select ?p where {
                        { ?p rdf:type rdf:Property } 
                        UNION { ?p rdf:type owl:ObjectProperty }
                        UNION { ?p rdf:type owl:DatatypeProperty } .
                    }
                """)
        for p, in resp:
            uri = str(p)
            pdict = make_dict(g.predicate_objects(p))

            if RDFS.label not in pdict:
                label = None
            else:
                labels = [x for x in pdict[RDFS.label] if x.language in accepted_languages]
                if len(labels) == 0:
                    label = None
                else:
                    assert len(labels) == 1, labels
                    label = labels[0].value

            domains = [str(x) for x in pdict.get(RDFS.domain, [])]

            if OWL.DatatypeProperty in pdict[RDF.type]:
                assert OWL.ObjectProperty not in pdict[RDF.type]
                ptype = OntPredicateType.DataProperty
            elif OWL.ObjectProperty in pdict[RDF.type]:
                ptype = OntPredicateType.ObjectProperty
            else:
                ptype = OntPredicateType.Unspecified

            if ptype == OntPredicateType.DataProperty:
                ranges = []
            else:
                ranges = [str(x) for x in pdict.get(RDFS.range, [])]

            predicates[uri] = OntPredicate(uri, label, domains, ranges, ptype)
        predicates['http://www.w3.org/2000/01/rdf-schema#label'] = OntPredicate('http://www.w3.org/2000/01/rdf-schema#label', 'label', [], [], OntPredicateType.DataProperty)

        # add parent, domains and ranges to the list if they don't have any definitions
        for p in predicates.values():
            for d in chain(p.domains, p.ranges):
                if d not in classes:
                    classes[d] = OntClass(d, None, [], no_def=True)
            p.domains = [classes[x] for x in p.domains]
            p.ranges = [classes[x] for x in p.ranges]

        for cid in list(classes.keys()):
            c = classes[cid]
            for pc in c.subClassOf:
                if pc not in classes:
                    classes[pc] = OntClass(pc, None, [], no_def=True)

        for c in classes.values():
            c.subClassOf = [classes[p] for p in c.subClassOf]
        cls._init_parents(classes)

        return cls(classes, predicates, prefixes, [str(infile)])

    @classmethod
    def _init_parents(cls, classes: Dict[str, OntClass]):
        """Update parent property of class"""
        def list_all_parents(c, parents):
            for p in c.subClassOf:
                parents.add(p.uri)
                list_all_parents(p, parents)

        for c in classes.values():
            list_all_parents(c, c.parents)

    def c(self, uri):
        """Get class by URI"""
        return self.classes[uri]

    def p(self, uri):
        """Get predicate by URI"""
        return self.predicates[uri]

    def iter_classes(self, ignore_nodef: bool=True, ignore_nolabel: bool=True):
        """Iter classes in the ontology, ignore class that aren't part of the ontology (nodef) or no label by default"""
        for c in self.classes.values():
            if ignore_nodef and c.no_def is False:
                continue
            if ignore_nolabel and c.label is None:
                continue

            yield c

    def iter_predicates(self):
        """Iterate predicates in the ontology"""
        return self.predicates.values()

    def get_rel_uri(self, uri):
        match_prefix = None
        longest_ns = ""
        for prefix, ns in self.prefixes.items():
            if uri.startswith(ns) and len(ns) > len(longest_ns):
                longest_ns = ns
                match_prefix = prefix
        if longest_ns == "":
            raise ValueError("Cannot convert the absolute uri into relative uri because of known namespace")

        return uri.replace(longest_ns, match_prefix + ":")

    def get_abs_uri(self, uri):
        prefix, reluri = uri.split(":", 1)
        return self.prefixes[prefix] + reluri

    def merge(self, ont: 'Ontology'):
        self.classes.update(ont.classes)
        self.predicates.update(ont.predicates)
        self.prefixes.update(ont.prefixes)
        self.infiles += ont.infiles

    def to_ttl_file(self, outfile: Union[str, Path]):
        g = rdflib.Graph()
        for c in self.classes.values():
            curi = URIRef(c.uri)
            g.add((curi, RDF.type, RDFS.Class))
            if c.label is not None:
                g.add((curi, RDFS.label, Literal(c.label)))
            for subClassOf in c.subClassOf:
                g.add((curi, RDFS.subClassOf, URIRef(subClassOf.uri)))
        for p in self.predicates.values():
            puri = URIRef(p.uri)
            if p.type == OntPredicateType.Unspecified:
                g.add((puri, RDF.type, RDF.Property))
            elif p.type == OntPredicateType.DataProperty:
                g.add((puri, RDF.type, OWL.DatatypeProperty))
            elif p.type == OntPredicateType.ObjectProperty:
                g.add((puri, RDF.type, OWL.ObjectProperty))
                for range in p.ranges:
                    g.add((puri, RDFS.range, URIRef(range.uri)))
            else:
                raise Exception("Unreachable!")

            for domain in p.domains:
                g.add((puri, RDFS.domain, URIRef(domain.uri)))
            g.add((puri, RDFS.label, Literal(p.label)))
        for prefix, uri in self.prefixes.items():
            g.namespace_manager.bind(prefix, Namespace(uri))
        g.serialize(str(outfile), format='ttl')


if __name__ == '__main__':
    ont = Ontology.from_file("/home/rook/workspace/sm-dev/data/dbpedia/ontology/dbo.20200420.ttl")