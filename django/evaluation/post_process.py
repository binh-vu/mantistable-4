"""Post process the output folder"""
import glob
import os
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Tuple, Dict
from urllib.parse import urlparse
from sm_unk.prelude import M, I
from kg_data.wikidata.wikidatamodels import QNode

class CPAMethod(Enum):
    Mantis = "mantis"
    Majority = "majority"
    MaxConfidence = "max-confidence"


@dataclass
class Input:
    table: I.ColumnBasedTable
    col_tags: List[str]
    linkage: List[Tuple[str, list, float]]


def get_cpa(method, col_tags, linkage):
    """This function follow the way mantistable4 implemented in CPA/export_pred
    Return the CPA in the following format: [(source_column_index, target_column_index, predicate)]
    """
    if len(linkage) == 0 or len(linkage[0]) == 0:
        return []

    # whether we should use mantis method that just use the first row
    # available option: mantis, majority, max-confidence
    if method == CPAMethod.Mantis:
        # they use the first row, just remove other row and we can reuse the code
        linkage = linkage[:1]

    subj_idx = None
    # map from index in the linkage to real column index
    target_indice = {}
    for real_ci, tag in enumerate(col_tags):
        if tag == 'SUBJ':
            assert subj_idx is None
            subj_idx = real_ci
        else:
            target_indice[len(target_indice)] = real_ci

    col_rels = {real_ci: {} for real_ci in target_indice.values()}
    for subj_ent, other_col_rels, ent_link_confidence in linkage:
        if subj_ent is None:
            assert len(other_col_rels) == 0
            continue
        for ci, real_ci in target_indice.items():
            target_col_rel, confidence = other_col_rels[ci]
            if target_col_rel is not None:
                prop = target_col_rel[1]
                if prop not in col_rels[real_ci]:
                    col_rels[real_ci][prop] = []
                col_rels[real_ci][prop].append(confidence)

    cpa = []
    for real_ci, preds in col_rels.items():
        if len(preds) == 0:
            continue
        if method == CPAMethod.Majority:
            correct_prop = max(preds.items(), key=lambda x: len(x[1]))
        else:
            correct_prop = max(preds.items(), key=lambda x: sum(x[1]))
        cpa.append((subj_idx, real_ci, correct_prop[0]))
    return cpa


def get_cta(inputs: List[Input], qnodes: Mapping[str, QNode]):
    """Run get cta task according to CTA/README.txt:
    1. generate cache of concepts
    2. create a file of candidate CTA
    3. run cta.py

    To avoid calling lamAPI, we implement the first step ourself
    """
    # save cache
    print(">>> CTA create cache")
    selected_qnode_ids = set()
    for input in inputs:
        for subj_ent, col_rels, confidence in input.linkage:
            if subj_ent is not None:
                selected_qnode_ids.add(subj_ent)
            for col_rel, col_rel_confidence in col_rels:
                if col_rel is not None:
                    s, p, o = col_rel
                    # heuristic to detect object property!
                    if len(o) > 1 and o[0] == "Q" and o[1].isdigit():
                        selected_qnode_ids.add(o)
                    assert s == subj_ent

    cache = {}
    for qnode_id in selected_qnode_ids:
        qnode = qnodes[qnode_id]
        concepts = {
            stmt.value.as_qnode_id()
            for stmt in qnode.props.get("P31", [])
        }
        cache[qnode_id] = list(concepts)

    # don't need to save to the file
    # outfile = EVAL_DIR.parent.parent / "CTA/cache.json"
    # M.serialize_json(cache, outfile)

    print(">>> CTA create candidate CTA")
    from generate_candidates_cta import get_concepts

    cta = {}
    for input in inputs:
        subj_idx = None
        # map from real column index to index in the linkage
        target_indice = {}
        for real_ci, tag in enumerate(input.col_tags):
            if tag == 'SUBJ':
                assert subj_idx is None
                subj_idx = real_ci
            else:
                target_indice[real_ci] = len(target_indice)

        # columns we run CTA on is the NE and subjects to list of entities
        # map from column to entity
        cta_cols = {}
        for real_ci, coltag in enumerate(input.col_tags):
            assert coltag in {"NE", "SUBJ", "LIT"}
            if coltag in {"NE", "SUBJ"}:
                cta_cols[real_ci] = set()

        for subj_ent, col_rels, confidence in input.linkage:
            if subj_ent is not None:
                cta_cols[subj_idx].add(subj_ent)
            else:
                assert len(col_rels) == 0
                continue
            for real_ci, ci in target_indice.items():
                col_rel, col_rel_confidence = col_rels[ci]
                if col_rel is not None:
                    s, p, o = col_rel
                    # heuristic to detect object property!
                    if len(o) > 1 and o[0] == "Q" and o[1].isdigit():
                        cta_cols[real_ci] = o
                    assert s == subj_ent

        # now convert the list of entities in those CTA columns into concepts
        for real_ci in cta_cols:
            global_concepts = get_concepts(cache, cta_cols[real_ci])
            if len(global_concepts) == 0:
                continue
            if input.table.metadata.table_id not in cta:
                cta[input.table.metadata.table_id] = {}
            cta[input.table.metadata.table_id][real_ci] = global_concepts

    ROOT_DIR = Path(os.path.abspath(__file__)).parent.parent.parent.absolute()
    outfile = ROOT_DIR / "CTA/candidates_cta.json"
    M.serialize_json(cta, outfile, indent=4)

    print(">>> CTA run CTA")
    subprocess.check_call("python CTA.py", shell=True, cwd=outfile.parent)

    cta_result = {}
    for tbl_id, column_index, concept in M.deserialize_csv(ROOT_DIR / "CTA/cta.csv"):
        if tbl_id not in cta_result:
            cta_result[tbl_id] = {}
        cta_result[tbl_id][column_index] = concept
    return cta_result


if __name__ == '__main__':
    from main import I, M, get_table_id, read_dataset
    EVAL_DIR = Path(os.path.abspath(__file__)).parent.absolute()
    dataset_dir = EVAL_DIR / "semtab2020_subset"
    args, qnodes = read_dataset(dataset_dir)

    # load inputs
    inputs = []
    for tbl, links in args:
        tbl_id = tbl.metadata.table_id
        infile = dataset_dir / "outputs" / f"{get_table_id(tbl_id)}.json"

        resp = M.deserialize_json(infile)
        assert tbl_id == resp['table_id']
        # norm the linkage format since json dump tuple to list
        linkage = [tuple(r) for r in resp['linkage']]

        inputs.append(Input(tbl, resp['column_tag'], linkage))

    (dataset_dir / "norm_outputs").mkdir(exist_ok=True)
    for cpa_method in CPAMethod:
        M.serialize_json({
            input.table.metadata.table_id: get_cpa(cpa_method, input.col_tags, input.linkage)
            for input in inputs
        }, dataset_dir / f"norm_outputs/tables.cpa.{cpa_method.value}.json")
    M.serialize_json(get_cta(inputs, qnodes), dataset_dir / "norm_outputs/tables.cta.json")