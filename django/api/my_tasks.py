# TODO: just for testing
# from mantistable.celery import app
# from api.models import Job
from api.process.utils import table
from api.process.utils.rules import person_rule as rules
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner, cleaner_light
from api.process.column_analysis import column_classifier, subject_detection
from api.process.data_retrieval import cells as cells_data_retrieval
from api.process.data_retrieval import links as links_data_retrieval

from api.process.cea import cea
from api.process.cea.models.cell import Cell
from api.process.cpa import cpa
from api.process.revision import revision
from api.process.utils.lamapi.my_wrapper import LamAPIWrapper as MyLamAPIWrapper
from api.process.utils.assets import Assets

# from celery import group
from multiprocessing import Manager

import mantistable.settings
import os
import mmap
import requests
import json
import math
import concurrent.futures
from collections import OrderedDict 

JOB_BACKEND = OrderedDict(
    [
        ('host', '35.236.42.69'), ('port', 8093), ('accessToken', 'ee4ba0c4f8db0eb3580cb3b7b5536c54')
    ]
)

# # TODO: EXtract constants
# THREADS = 15
# manager = Manager()

# # TODO: Extract to utils
# def generate_chunks(iterable, n):
#     assert (n > 0)
#     for i in range(0, len(iterable), n):
#         yield iterable[i:i + n]


def job_slot(job_id: int):
    print(f"Inside job_slot: params are: {job_id}")
    # job = Job.objects.get(id=job_id)

    tables = [
        (table_id, table_name, table_data)
        for table_id, table_name, table_data in job.tables
    ]

    workflow = data_preparation_phase.s(tables, job_id) | data_retrieval_phase.s(job_id) | computation_phase.s(job_id) | clean_up.si(job_id)
    return workflow.apply_async()

# @app.task(name="data_preparation_phase", bind=True)
def data_preparation_phase(tables, job_id):
    # self.replace(group([
    #     data_preparation_table_phase.s(job_id, *table)
    #     for table in tables
    # ]))
    return [
        data_preparation_table_phase(job_id, *table)
        for table in tables
    ]

# @app.task(name="data_preparation_table_phase")
def data_preparation_table_phase(job_id, table_id, table_name, table_data):
    # job = Job.objects.get(id=job_id)

    print(f"Normalization")
    normalization_result = _normalization_phase(table_id, table_data)
    # client_callback(job, table_id, "normalization", normalization_result)
    
    print(f"Column Analysis")
    col_analysis_result = _column_analysis_phase(table_id, table_name, table_data, normalization_result)

    print(f"Subject detection")
    col_analysis_result = _subject_detection_phase(table_id, table_name, table_data, col_analysis_result)
    # client_callback(job, table_id, "column analysis", col_analysis_result)

    print(col_analysis_result)
    return table_id, table_data, col_analysis_result

def _normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()
    return metadata

def _column_analysis_phase(table_id, table_name, table_data, data):
    stats = {
        col_name: d["stats"]
        for col_name, d in data.items()
    }

    table_model = table.Table(table_id=table_id, table=table_data)
    cea_targets = Assets().get_json_asset("ne_cols.json")
    targets = cea_targets.get(table_name, [])
    
    if len(targets) > 0:
        cc = column_classifier.ColumnClassifierTargets(table_model.get_cols(), stats, targets)
    else:
        cc = column_classifier.ColumnClassifier(table_model.get_cols(), stats)
    
    tags = cc.get_columns_tags()
    
    metadata = {
        col_name: {**prev_meta, **tags[col_name]}
        for col_name, prev_meta in data.items()
    }

    return metadata

def _subject_detection_phase(table_id, table_name, table_data, metadata):
    table_model = table.Table(table_id=table_id, table=table_data)

    subject_idx = subject_detection.get_subject_col_idx(table_model, list(metadata.values()))

    for idx, (col_name, meta) in enumerate(metadata.items()):
        if idx == subject_idx:
            metadata[col_name]["tags"]["col_type"] = "SUBJ"

    """
    metadata = {
        col_name: {
            **meta,
            "is_subject": idx == subject_idx
        }
        for idx, (col_name, meta) in enumerate(metadata.items())
    }
    """

    return metadata


# @app.task(name="data_retrieval_phase", bind=True)
def data_retrieval_phase(tables, job_id):
    # job = Job.objects.get(id=job_id)
    # job.progress["current"] = 1
    # job.save()

    print(f"Data retrieval")

    cells_content = set()
    for table in tables:
        metadata = table[2]
        tags = [
            col_val["tags"]["col_type"]
            for col_val in metadata.values()
        ]

        for col_idx, (col_name, col_val) in enumerate(metadata.items()):
            assert MyLamAPIWrapper.column_name2index[col_name] == col_idx
            for row_idx, values in enumerate(col_val["values"]):
                if tags[col_idx] != "LIT":
                    # Apply rules
                    # TODO:
                    """
                    rule = rules.PersonRule(values["original"])
                    if rule.match():
                        query = rule.build_query()
                    else:
                        query = values["normalized"]
                    """
                    query = values["normalized"]

                    # telling our lamAPI that this query belong to which cell
                    MyLamAPIWrapper.track_normed_cell(row_idx, col_idx, query)

                    cells_content.add(query)
    
    cells_content = list(cells_content)
    print("Data retrieval result!")
    # NOTE: this func will not return anything
    data_retrieval_group_phase(job_id, cells_content)

# TODO: duplicate? 
# # @app.task(name="computation_phase", bind=True)
# def computation_phase(info, job_id):
#     print(f"Inside computation_phase: params are: {info}, {job_id}")
#     # job = Job.objects.get(id=job_id)
#     # job.progress["current"] = 2
#     # job.save()

#     print("Computation")
#     # self.replace(
#     #     group([
#     #         computation_table_phase.s(job_id, *table)
#     #         for table in info
#     #     ])
#     # )
#     return [
#         computation_table_phase(job_id, *table)
#         for table in info
#     ]

# @app.task(name="data_retrieval_group_phase")
def data_retrieval_group_phase(job_id, chunk):
    # job = Job.objects.get(id=job_id)
    # TODO: this is where lamAPI is accessed: django/api/process/data_retrieval/cells.py
    # NOTE: this func does not return anything: simply update candidate.index in media/
    cells_data_retrieval.CandidatesRetrieval(chunk, JOB_BACKEND).write_candidates_cache()

"""
@app.task(name="data_retrieval_links_phase", bind=True)
def data_retrieval_links_phase(self, job_id, tables):
    print(f"Data retrieval links")
    candidates = shared_memory

    def get_candidates(raw_cell, norm_cell):
        rule = rules.PersonRule(raw_cell)
        if rule.match():
            query = rule.build_query()
        else:
            query = norm_cell

        return candidates.get(query, [])

    pairs = {}
    for table in tables:
        metadata = table[2]
        tags = [
            col_val["tags"]["col_type"]
            for col_val in metadata.values()
        ]

        rows = []
        for row_idx in range(0, len(metadata[list(metadata.keys())[0]]["values"])):
            rows.append([])
            for col_idx in range(0, len(metadata.keys())):
                values = metadata[list(metadata.keys())[col_idx]]["values"][row_idx]
                rows[-1].append(values)

        for values in rows:
            subject_cell_raw = values[0]["original"]
            subject_norm = values[0]["normalized"]
            subject_candidates = get_candidates(subject_cell_raw, subject_norm)
            object_cells = values[1:]

            for idx, obj_cell in enumerate(object_cells):
                obj_raw = obj_cell["original"]
                
                if tags[idx + 1] != "LIT":
                    obj_norm = obj_cell["normalized"]
                else:
                    obj_norm = obj_raw

                obj_candidates = get_candidates(obj_raw, obj_norm)

                pair = (
                    (subject_cell_raw, subject_norm, subject_candidates, False),
                    (obj_raw, obj_norm, obj_candidates, tags[idx + 1] == "LIT"),
                )
                pairs[(subject_cell_raw, obj_raw)] = pair
    
    CHUNK_SIZE = 20
    chunks = generate_chunks(list(pairs.values()), CHUNK_SIZE)

    self.replace(
        group([
            data_retrieval_links_group_phase.si(job_id, chunk)
            for chunk in chunks
        ]) | dummy_phase.s(tables)
    )
"""

# TODO: this function is never called ?
# @app.task(name="data_retrieval_links_group_phase")
def data_retrieval_links_group_phase(job_id, chunk):
    # job = Job.objects.get(id=job_id)
    links = links_data_retrieval.LinksRetrieval(chunk, JOB_BACKEND).get_links()

    links = {
        hash(k): v
        for k,v in links.items()
    }

    return links

"""
@app.task(name="dummy_phase")
def dummy_phase(triples, tables):
    joined_triples = {}
    for triple in triples:
        joined_triples.update(triple)

    return tables, joined_triples
"""

# @app.task(name="dummy_phase")
def dummy_phase(tables):
    return tables

# @app.task(name="computation_phase", bind=True)
def computation_phase(info, job_id):
    tables = info
    # job = Job.objects.get(id=job_id)
    # job.progress["current"] = 2
    # job.save()

    print("Computation")
    # self.replace(
    #     group([
    #         computation_table_phase.s(job_id, *table)
    #         for table in tables
    #     ])
    # )
    return [
      computation_table_phase(job_id, *table)
      for table in tables
    ]

# @app.task(name="computation_table_phase")
def computation_table_phase(job_id, table_id, table_data, columns):
    # job = Job.objects.get(id=job_id)

    print("Computation table")
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    # NOTE: see CacheWriter in django/api/process/data_retrieval/cells.py
    # candidates.index is a dict of label (key): [offset, size] (value)
    # candidates.map is the real data stored as list(queried_label, uri)\n

    candidates_index = {}
    with open(os.path.join(mantistable.settings.MEDIA_ROOT, "candidates.index"), "r") as f_idx:
        candidates_index = json.loads(f_idx.read())

    candidates = {}
    with open(os.path.join(mantistable.settings.MEDIA_ROOT, "candidates.map"), "r") as f:
        candidates_map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        for norm in normalized.values():
            info = candidates_index.get(norm, None)
            if info is not None:
                offset = info[0]
                size = info[1]
                candidates_map.seek(offset)
                candidates[norm] = json.loads(candidates_map.read(size))

    print ("CEA")
    cea_results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        #triples=triples,
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute(JOB_BACKEND)

    print(cea_results)

    print ("CPA")
    cpa_results = cpa.CPAProcess(
        cea_results
    ).compute()

    print(cpa_results)

    print ("Revision")
    revision_results = revision.RevisionProcess(
        cea_results, 
        cpa_results
    ).compute()
    # client_callback(job, table_id, "computation", revision_results)

    # also return the tag and the order of columns to check, since python 3.7 the dictionarry keep the insert order so we don't actually need it
    return revision_results, tags, list(columns.keys())
    #return table_id, table_data, revision_results


# @app.task(name="clean_up")
def clean_up(job_id):
    print(f"Inside clean_up: params are: {job_id}")
    # job = Job.objects.get(id=job_id)
    # job.progress["current"] = 3
    # job.save()

    # client_callback(job, -1, "end", {})

    # TODO: For now just delete job
    # job.delete()

# ====================================================

def client_callback(job, table_id, header: str, payload=None):
    assert header is not None and len(header) > 0   # Contract

    if payload is None:
        payload = { }

    message = {
        "job_id": job.id,
        "table_id": table_id,
        "header": header,
        "payload": payload
    }

    try:
        response = requests.post(job.callback, json=message)
        print("sent to:", job.callback)
    except Exception as e:
        # Send to morgue?
        print("[api.my_tasks] Error", e)
        return

    if response.status_code != 200:
        # Send to morgue?
        print("Response error", response.status_code)
        pass
