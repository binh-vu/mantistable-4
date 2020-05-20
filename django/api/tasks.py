from mantistable.celery import app
from api.models import Job
from api.serializers import TableSerializer
from api.process.utils import table
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner
from api.process.column_analysis import column_classifier
from api.process.data_retrieval import cells as data_retrieval

from api.process.cea import cea

#from app.models import Table

from celery.result import allow_join_result
from celery import group, chord, chain
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
            label = res["label"]     # TODO: I need this information
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

    results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute()

    print(results)

    return table_id, table_data#, results

""" NOTE: Test with chunks
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
    data_retrieval_task = data_retrieval_phase.s(
        computation_phase.s()
    )

    workflow = chain(data_preparation_task, data_retrieval_task)
    return workflow.apply_async()


@app.task(name="data_preparation_phase")
def data_preparation_phase(table_id, table_data):
    # Normalization
    table_model = table.Table(table_id=table_id, table=table_data)
    normalization_metadata = normalizer.Normalizer(table_model).normalize()

    # Column analysis
    stats = {
        col_name: d["stats"]
        for col_name, d in normalization_metadata.items()
    }
    cc = column_classifier.ColumnClassifier(stats)
    tags = cc.get_columns_tags()
    
    col_analysis_metadata = {
        col_name: {**prev_meta, **tags[col_name]}
        for col_name, prev_meta in normalization_metadata.items()
    }

    return table_id, table_data, col_analysis_metadata

@app.task(name="data_retrieval_phase")
def data_retrieval_phase(info, callback):
    def generate_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    tags = [
        col_val["tags"]["col_type"]
        for data in info
        for col_val in data[2].values()
    ]
    cells = list({
        values["normalized"]
        for data in info
        for col_idx, col_val in enumerate(data[2].values())
        for values in col_val["values"]
        if tags[col_idx] != "LIT"
    })

    chunk_size = 10
    chunks = list(generate_chunks(cells, max(len(cells) // chunk_size, 1)))

    chunk_task = group([
        data_retrieval_phase_chunk.s(chunk)
        for chunk in chunks
    ])

    if callback is not None:
        callback["args"].append(info)
        flow = chunk_task | callback
        flow.apply_async()

    return chunks


@app.task(name="data_retrieval_phase_chunk")
def data_retrieval_phase_chunk(cells):
    solr_result = data_retrieval.CandidatesRetrieval(cells).get_candidates()

    results = {}
    for cell in solr_result.keys():
        for res in solr_result[cell]:
            label = res["label"]     # TODO: I need this information
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in results:
                results[cell] = []

            results[cell].append((norm_label, entity))

    return results

@app.task(name="computation_phase")
def computation_phase(candidates_list, col_analysis_metadata):
    candidates = {}
    for cand_chunk in candidates_list:
        candidates.update(cand_chunk)

    group([
        computation_table_phase.s(*table_metadata, candidates)
        for table_metadata in col_analysis_metadata
    ]).apply_async()

@app.task(name="computation_table_phase")
def computation_table_phase(table_id, table_data, columns, candidates):
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute()

    #print(results)

    return table_id, table_data#, results
"""

# NOTE: Original implementation
"""
def sync_group_task(task_ref, data: list, *args):
    assert(isinstance(data, list))

    with allow_join_result():
        task = group([
            task_ref.si(*d, *args)
            for d in data
        ]).apply_async()
        result = task.join()

    return result

def sync_chunk_task(task_ref, data: list, chunk_size=10):
    assert(isinstance(data, list))

    with allow_join_result():
        task = task_ref.chunks(data, min(len(data), chunk_size)).group().apply_async()
        result = task.join()

    return result[0][0] # Wat the heck?


@app.task(name="job_slot")
def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)
    rows = [
        (idx, table)
        for idx, table in enumerate(job.tables)
    ]

    # TODO: Do not make sync tasks, make it async (use chains)
    norm_result  = sync_group_task(normalization_phase, rows)
    #rest_hook(job_id, norm_result)

    colan_result = sync_group_task(column_analysis_phase, norm_result)
    #rest_hook(job_id, colan_result)

    dr_result    = sync_chunk_task(data_retrieval_phase, colan_result)
    #rest_hook(job_id, dr_result)

    comp_result  = sync_group_task(computation_phase, colan_result, dr_result)
    #rest_hook(job_id, comp_result)

    #Repository().write_cols(colan_result)

    return job_id, None

@app.task(name="normalization_phase")
def normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()

    return table_id, data, metadata


@app.task(name="column_analysis_phase")
def column_analysis_phase(table_id, table, data):
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

    return table_id, table, metadata


@app.task(name="data_retrieval_phase")
def data_retrieval_phase(table_id, table, data):
    tags = [
        col_val["tags"]["col_type"]
        for col_val in data.values()
    ]
    cells = [
        values["normalized"]
        for col_idx, col_val in enumerate(data.values())
        for values in col_val["values"]
        if tags[col_idx] != "LIT"
    ]
    solr_result = data_retrieval.CandidatesRetrieval(cells).get_candidates()

    results = {}
    for cell in solr_result.keys():
        for res in solr_result[cell]:
            label = res["label"]     # TODO: I need this information
            entity = res["uri"]

            norm_label = cleaner.Cleaner(label).clean()
            if cell not in results:
                results[cell] = []

            results[cell].append((norm_label, entity))

    return results


@app.task(name="computation_phase")
def computation_phase(table_id, table_data, columns, candidates):
    tags = [
        col_val["tags"]["col_type"]
        for col_val in columns.values()
    ]

    normalized = {
        values["original"]: values["normalized"]
        for col_val in columns.values()
        for values in col_val["values"]
    }

    results = cea.CEAProcess(
        table.Table(table_id=table_id, table=table_data),
        tags=tags,
        normalized_map=normalized,
        candidates_map=candidates
    ).compute()

    #print(results)

    return table_id, table_data, results

@app.task(name="rest_hook")
def rest_hook_task(data):
    #rest_hook(*data)
    return data

def rest_hook(job_id, results):
    job = Job.objects.get(id=job_id)

    try:
        requests.post(job.callback, data={
            'job_id': job.id,
            'progress': json.dumps(job.progress),
            'results': results
        })
    except requests.exceptions.InvalidSchema as e:
        print(e)
    except requests.exceptions.ConnectionError as e:
        print(e)

    job.progress["current"] += 1
    job.save()

    if job.progress["total"] <= 0 or (job.progress["total"] > 0 and job.progress["current"] == job.progress["total"]):
        job.delete()
"""