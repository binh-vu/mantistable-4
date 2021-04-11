import subprocess
import sys, os, json, glob, uuid
from dataclasses import dataclass
from enum import Enum
from hashlib import md5
from operator import itemgetter
from typing import Dict, Tuple, List, Set, Optional
from pathlib import Path
from urllib.parse import urlparse

from tqdm.auto import tqdm


from api.my_tasks import data_preparation_phase, data_retrieval_phase, computation_phase, clean_up
from api.process.utils.lamapi.my_wrapper import LamAPIWrapper
from sm_unk.prelude import M, I
from kg_data.wikidata.wikidatamodels import *


def get_table_id(table_id: str):
    if table_id.find("dbpedia.org") == -1:
        return table_id
    id = urlparse(table_id).path.replace("/resource/", "").replace("/", "_")
    return id + "_" + md5(table_id.encode()).hexdigest()


def run_prediction(qnodes: Dict[str, QNode], table_idx, tbl: I.ColumnBasedTable, links: Dict[Tuple[int, int], List[str]], subjs: Optional[Set[Tuple[int, str]]]):
    table_id = get_table_id(tbl.metadata.table_id)

    # convert the table into mantis format
    mantis_table = []
    has_unique_label = len({c.name for c in tbl.columns}) == len(tbl.columns)
    if has_unique_label:
        name2index = {c.name: ci for ci, c in enumerate(tbl.columns)}
    else:
        name2index = {f"{c.name} {ci}": ci for ci, c in enumerate(tbl.columns)}
    if subjs is not None:
        assert all([tbl.columns[ci].name == cname for ci, cname in subjs])
        subjs = {ci for ci, cname in subjs}

    for ri in range(len(tbl.columns[0].values)):
        row = {}
        for ci, col in enumerate(tbl.columns):
            cname = col.name if has_unique_label else f"{col.name} {ci}"
            row[cname] = col.values[ri]
        mantis_table.append(row)

    # telling our lamapiwrapper we are working on this table
    LamAPIWrapper.set_table(tbl, name2index, links, qnodes)

    # submit the job
    job_id = str(uuid.uuid4())
    workflow_tables = data_preparation_phase([(table_idx, table_id, mantis_table)], job_id)
    assert len(workflow_tables) == 1

    # force the result of subject column detection to be correct if subjs is supplied
    if subjs is not None:
        assert sum(int(cdata['tags']['col_type'] == 'SUBJ') == 1 for cname, cdata in workflow_tables[0][2].items()), "My understanding about only one subjects in the table of this method is incorrect"
        if any(cdata['tags']['col_type'] == 'SUBJ' and name2index[cname] not in subjs
            for cname, cdata in workflow_tables[0][2].items()):
            # they incorrectly recognize the subject column, force the result to be correct
            subj = sorted(subjs)[0]
            for cname, cdata in workflow_tables[0][2].items():
                cindex = name2index[cname]
                if cindex == subj:
                    cdata['tags']['col_type'] = 'SUBJ'
                elif cdata['tags']['col_type'] in {"SUBJ", "NE"}:
                    cdata['tags']['col_type'] = 'NE'

    data_retrieval_phase(workflow_tables, job_id)
    result = computation_phase(workflow_tables, job_id)
    assert len(result) == 1

    # get result
    linkage, column_tag, column_names = result[0]
    assert column_names == [name for name, index in sorted(name2index.items(), key=itemgetter(1))]

    # write the result
    scenario = "default"
    if subjs is not None:
        scenario = "oracle_subj"
    outfile = dataset_dir / f"predictions_{scenario}" / f"{table_id}.json"
    outfile.parent.mkdir(exist_ok=True)
    if outfile.exists():
        assert M.deserialize_json(outfile)['table_id'] == tbl.metadata.table_id
    M.serialize_json({
        "table_id": tbl.metadata.table_id,
        "linkage": linkage,
        "column_tag": column_tag,
    }, outfile, indent=4)


def read_dataset(dataset_dir, oracle_subjs: bool = False):
    if dataset_dir.name == "500tables":
        args = []
        for o in M.deserialize_jl(dataset_dir / "inputs.jl"):
            table = I.ColumnBasedTable.from_json(o['table'])
            links = {}
            for ri, rlinks in enumerate(o['links']):
                for ci, clinks in enumerate(rlinks):
                    qnode_ids = [link['qnode_id'] for link in clinks if link['qnode_id'] is not None]
                    if len(qnode_ids) > 0:
                        links[ri, ci] = qnode_ids
            args.append((table, links, o['subjs'] if oracle_subjs else None))
    elif dataset_dir.name == "250tables":
        args = []
        for o in M.deserialize_jl(dataset_dir / "inputs.jl"):
            table = I.ColumnBasedTable.from_json(o['table'])
            links = {}
            for ri, rlinks in enumerate(o['links']):
                for ci, clinks in enumerate(rlinks):
                    qnode_ids = [link['qnode_id'] for link in clinks if link['qnode_id'] is not None]
                    if len(qnode_ids) > 0:
                        links[ri, ci] = qnode_ids
            args.append((table, links, o['subjs'] if oracle_subjs else None))
    elif dataset_dir.name == 'semtab2020_subset':
        args = []
        for o in M.deserialize_jl(dataset_dir / "tables.size=512.repeat=4.seed=1212.jl"):
            o = o['table']
            table = I.ColumnBasedTable.from_json(o['table'])
            links = {}
            for ri, rlinks in enumerate(o['links']):
                for ci, clinks in enumerate(rlinks):
                    qnode_ids = [link['qnode_id'] for link in clinks if link['qnode_id'] is not None]
                    if len(qnode_ids) > 0:
                        links[ri, ci] = qnode_ids
            args.append((table, links, None))
    elif dataset_dir.name == 'semtab2020':
        args = []
        for o in M.deserialize_jl(dataset_dir / "inputs.jl"):
            table = I.ColumnBasedTable.from_json(o['table'])
            links = {}
            for ri, rlinks in enumerate(o['links']):
                for ci, clinks in enumerate(rlinks):
                    qnode_ids = [link['qnode_id'] for link in clinks if link['qnode_id'] is not None]
                    if len(qnode_ids) > 0:
                        links[ri, ci] = qnode_ids
            args.append((table, links, o['subjs'] if oracle_subjs else None))
    else:
        assert False

    qnodes = {
        k: QNode.deserialize(v)
        for fpath in glob.glob(str(dataset_dir / "qnodes/*.gz"))
        for k, v in M.deserialize_key_val_lines(fpath)
    }
    return args, qnodes


if __name__ == "__main__":
    from evaluation.post_process import Input, CPAMethod, get_cpa, get_cta
    EVAL_DIR = Path(os.path.abspath(__file__)).parent.absolute()
    dataset_dir = EVAL_DIR / "semtab2020_subset"
    dataset_dir = EVAL_DIR / "semtab2020"
    # dataset_dir = EVAL_DIR / "500tables"
    # dataset_dir = EVAL_DIR / "250tables"

    oracle_subjs = False
    args, qnodes = read_dataset(dataset_dir, oracle_subjs)

    scenario = "default"
    if oracle_subjs:
        scenario = "oracle_subj"

    # run prediction
    for tidx, (table, links, subjs) in enumerate(args):
        # if tidx != 13:
        #     continue
        # if table.metadata.table_id != 'http://dbpedia.org/resource/11th_Lok_Sabha?dbpv=2020-02&nif=table&ref=3.10_2&order=0':
        #     continue
        print(">>>> progress:", tidx, "/", len(args))
        # if tidx < 468:
        #     continue
        run_prediction(qnodes, tidx, table, links, subjs)
    # exit(0)
    # load predictions to inputs
    inputs = []
    for tbl, links, subjs in args:
        tbl_id = tbl.metadata.table_id
        infile = dataset_dir / f"predictions_{scenario}" / f"{get_table_id(tbl_id)}.json"

        resp = M.deserialize_json(infile)
        assert tbl_id == resp['table_id']
        # norm the linkage format since json dump tuple to list
        linkage = [tuple(r) for r in resp['linkage']]

        inputs.append(Input(tbl, resp['column_tag'], linkage))

    # (dataset_dir / f"outputs_{scenario}").mkdir(exist_ok=True)
    # for cpa_method in CPAMethod:
    #     M.serialize_json({
    #         input.table.metadata.table_id: get_cpa(cpa_method, input.col_tags, input.linkage)
    #         for input in inputs
    #     }, dataset_dir / f"outputs_{scenario}/tables.cpa.{cpa_method.value}.json")
    M.serialize_json(get_cta(inputs, qnodes), dataset_dir / f"outputs_{scenario}/tables.cta.json")

# tables payload example
# [
#   [
#     6, 
#     'test', 
#     [
#       {'MOUNTAIN': 'Mount Everest', 'HEIGHT IN METERS': '8,848', 'RANGE': 'Himalayas', 'CONQUERED ON': 'May 29, 1953', 'COORDINATES': '27.98785, 86.92502609999997', 'URL': 'https://en.wikipedia.org/wiki/Mount_Everest'}, {'MOUNTAIN': 'K-2 (Godwin Austin)', 'HEIGHT IN METERS': '8,611', 'RANGE': 'Karakoram', 'CONQUERED ON': 'July 31, 1954', 'COORDINATES': '35.8799875,76.51510009999993', 'URL': 'https://en.wikipedia.org/wiki/K2'}, {'MOUNTAIN': 'Kanchenjunga', 'HEIGHT IN METERS': '8,597', 'RANGE': 'Himalayas', 'CONQUERED ON': 'May 25, 1955', 'COORDINATES': '27.7024914,88.14753500000006', 'URL': 'https://en.wikipedia.org/wiki/Kangchenjunga'}
#     ]
#   ]
# ]
