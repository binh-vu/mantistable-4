import sys, os, json, glob, uuid
from typing import Dict
from pathlib import Path
from tqdm.auto import tqdm

EVAL_DIR = Path(os.path.abspath(__file__)).parent / "evaluation"
sys.path.append(str((EVAL_DIR).absolute()))

from api.my_tasks import data_preparation_phase, data_retrieval_phase, computation_phase, clean_up
from api.process.utils.lamapi.my_wrapper import LamAPIWrapper
from sm_unk.prelude import M, I


def run_prediction(qnodes: Dict[str, I.QNode], table_idx, table_name, table):
    tbl = I.ColumnBasedTable.from_json(table['table'])

    # convert the table into mantis format
    mantis_table = []
    has_unique_label = len({c.name for c in tbl.columns}) == len(tbl.columns)
    if has_unique_label:
        name2index = {c.name: ci for ci, c in enumerate(tbl.columns)}
    else:
        name2index = {f"{c.name} {ci}": ci for ci, c in enumerate(tbl.columns)}
    
    for ri in range(len(tbl.columns[0].values)):
        row = {}
        for ci, col in enumerate(tbl.columns):
            cname = col.name if has_unique_label else f"{col.name} {ci}"
            row[cname] = col.values[ri]
        mantis_table.append(row)

    # telling our lamapiwrapper we are working on this table
    LamAPIWrapper.set_table(tbl, name2index, {tuple(key): qnode_ids for key, qnode_ids in table['links']}, qnodes)

    # submit the job
    job_id = str(uuid.uuid4())
    workflow_tables = data_preparation_phase([(table_idx, table_name, mantis_table)], job_id)
    assert len(workflow_tables) == 1
    data_retrieval_phase(workflow_tables, job_id)
    workflow_tables = computation_phase(workflow_tables, job_id)
    assert len(workflow_tables) == 1


if __name__ == "__main__":
    tables = [(i, Path(x).stem, M.deserialize_json(x)) for i, x in enumerate(sorted(glob.glob(str(EVAL_DIR / "data/*.json"))))]
    qnodes = [
        I.QNode.deserialize(s)
        for fname in glob.glob(str(EVAL_DIR / "data/qnodes*jl.gz"))
        for s in M.deserialize_lines(fname)
    ]
    qnodes = {qnode.id: qnode for qnode in qnodes}

    for tidx, tname, table in tables:
        run_prediction(qnodes, tidx, tname, table)
        
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
