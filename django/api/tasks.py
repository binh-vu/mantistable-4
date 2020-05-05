from mantistable.celery import app
from api.models import Job
from api.serializers import TableSerializer
from api.process.utils import table
from api.process.utils.mongo.repository import Repository

from api.process.normalization import normalizer, cleaner
from api.process.column_analysis import column_classifier

#from api.process.cea import cea

#from app.models import Table

from celery.result import allow_join_result
from celery import group

import requests
import json

def sync_group_task(task_ref, data: list):
    assert(isinstance(data, list))

    with allow_join_result():
        task = group([
            task_ref.si(*d)
            for d in data
        ]).apply_async()
        result = task.join()

    return result

def sync_chunk_task(task_ref, data: list, chunk_size=10):
    assert(isinstance(data, list))

    with allow_join_result():
        task = task_ref.chunks(data, min(len(data), chunk_size)).group().apply_async()
        result = task.join()

    return result


@app.task(name="job_slot")
def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)
    rows = [
        (idx, table)
        for idx, table in enumerate(job.tables)
    ]

    # TODO: Do not make sync tasks, make it async (use chains)
    norm_result  = sync_group_task(normalization_phase, rows)
    rest_hook(job_id, norm_result)

    colan_result = sync_group_task(column_analysis_phase, norm_result)
    rest_hook(job_id, colan_result)

    dr_result    = sync_chunk_task(data_retrieval_phase, norm_result)
    rest_hook(job_id, dr_result)

    #comp_result  = sync_group_task(computation_phase.si, colan_result, dr_result)
    #rest_hook(job_id, comp_result)

    #Repository().write_cols(colan_result)

    return job_id, None

@app.task(name="normalization_phase")
def normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()

    return (table_id, metadata)

@app.task(name="column_analysis_phase")
def column_analysis_phase(table_id, data):
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

    return (table_id, metadata)

@app.task(name="data_retrieval_phase")
def data_retrieval_phase(table_id, data):
    # NOTE: Mockup
    solr_result = [
        ("Batman (mass measure)", "Batman_(unit)"),
        ("Batman car", "Batmobile"),
        ("List of municipalities in Batman Province", "List_of_municipalities_in_Batman_Province"),
        ("Batman Knight Flight", "Dominator_(roller_coaster)"),
        ("Batman", "Batman"),
        ("Gary Nolan (radio host)", "Gary_Nolan_(radio_host)"),
        ("David Nolan (libertarian)", "David_Nolan_(libertarian)"),
        ("Christopher Nolan", "Christopher_Nolan"),
        ("9537 Nolan", "9537_Nolan"),
    ]

    results = {}
    for label, entity in solr_result:
        normal_label = cleaner.Cleaner(label).clean()
        if normal_label not in results:
            results[normal_label] = []

        results[normal_label].append(entity)

    return results


@app.task(name="computation_phase")
def computation_phase(table_id, columns, candidates):
    cea.CEAProcess(
        normalized_map=normalized,
        candidates_map=candidates
    )

@app.task(name="rest_hook")
def rest_hook_task(data):
    rest_hook(*data)

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
