from mantistable.celery import app
from api.models import Job
from api.serializers import TableSerializer
from api.process.utils import table
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner
from api.process.column_analysis import column_classifier
from api.process.data_retrieval import cells as data_retrieval

from api.process.cea import cea
from api.process.cpa import cpa

from celery import group
from multiprocessing import Manager

import requests
import json

manager = Manager()
shared_memory = manager.dict()

def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)
    rows = [
        (idx, table)
        for idx, table in enumerate(job.tables)
    ]

    data_preparation_task = group([
        data_preparation_phase.s(*row)
        for row in rows
    ])

    workflow = data_preparation_task | computation_phase.s()
    return workflow.apply_async()


@app.task(name="data_preparation_phase")
def data_preparation_phase(table_id, table_data):
    print(f"Normalization")
    normalization_result = _normalization_phase(table_id, table_data)
    
    print(f"Column Analysis")
    col_analysis_result = _column_analysis_phase(table_id, table_data, normalization_result)

    print(f"Data retrieval")
    data_retrieval_result = _data_retrieval_phase(table_id, col_analysis_result)
    for cell, content in data_retrieval_result.items():
        shared_memory[cell] = content

    return table_id, table_data, col_analysis_result

@app.task(name="computation_phase")
def computation_phase(info):
    print("Computation")
    computation_task = group([
        computation_table_phase.s(*table)
        for table in info
    ])
    computation_task.apply_async()


@app.task(name="computation_table_phase")
def computation_table_phase(table_id, table_data, columns):
    print("Computation table")
    return _computation_phase(table_id, table_data, columns)


def _normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()

    return metadata

def _column_analysis_phase(table_id, table, data):
    stats = {
        col_name: d["stats"]
        for col_name, d in data.items()
    }
    cc = column_classifier.ColumnClassifier(stats)
    tags = cc.get_columns_tags()
    
    metadata = {
        col_name: {**prev_meta, **tags[col_name]}
        for col_name, prev_meta in data.items()
    }

    return metadata

def _data_retrieval_phase(table_id, data):
    tags = [
        col_val["tags"]["col_type"]
        for col_val in data.values()
    ]
    cells = [
        values["normalized"]
        for col_idx, col_val in enumerate(data.values())
        for values in col_val["values"]
        if tags[col_idx] != "LIT" and values["normalized"] not in shared_memory
    ]
    solr_result = data_retrieval.CandidatesRetrieval(cells).get_candidates()

    results = {}
    for cell in solr_result.keys():
        for res in solr_result[cell]:
            label = res["label"]
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in results:
                results[cell] = []

            results[cell].append((norm_label, entity))

    return results

def _computation_phase(table_id, table_data, columns):
    candidates = shared_memory
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    cea_results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute()

    print(cea_results)

    cpa_results = cpa.CPAProcess(
        cea_results
    ).compute()

    print(cpa_results)

    return table_id, table_data#, results