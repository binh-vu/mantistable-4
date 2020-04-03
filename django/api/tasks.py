from mantistable.celery import app
from api.models import Job
from api.serializers import TableSerializer
from api.process.utils import table
from api.process.utils.mongo.repository import Repository
from api.process.normalization import normalizer
from app.models import Table

from celery.result import allow_join_result
from celery import group
import requests
import json

def sync_group_task(task_sig, data: list):
    assert(isinstance(data, list))

    with allow_join_result():
        task = group([
            task_sig(d["id"], d["rows"])
            for d in data
        ]).apply_async()
        result = task.join()

    return result

def sync_chunk_task(task_sig, data: list, chunk_size=10):
    assert(isinstance(data, list))

    with allow_join_result():
        task = task_sig.chunks(data, min(len(data), chunk_size)).group().apply_async()
        result = task.join()

    return result


@app.task(name="job_slot")
def job_slot(job_id: int):
    job = Job.objects.get(id=job_id)
    tables = Table.objects.filter(id__in=job.table_ids)
    rows = [
        {
            "id": table.id,
            "rows": table.rows
        }
        for table in tables
    ]

    # TODO: Insert here cron for ETA
    norm_result  = sync_group_task(normalization_phase.si, rows)
    colan_result = sync_group_task(column_analysis_phase.si, rows)
    
    #dr_result    = sync_chunk_task(data_retrieval_phase, rows) # TODO: Not rows...
    comp_result  = sync_group_task(computation_phase.si, rows)

    # TODO: save to db with pymongo
    Repository().write_cols(norm_result)

    return job_id

@app.task(name="normalization_phase")
def normalization_phase(table_id, data):
    table_model = table.Table(table_id=table_id, table=data)
    metadata = normalizer.Normalizer(table_model).normalize()

    return metadata

@app.task(name="column_analysis_phase")
def column_analysis_phase(table_id, data):
    return None

@app.task(name="data_retrieval_phase")
def data_retrieval_phase(table_id, data):
    return None

@app.task(name="computation_phase")
def computation_phase(table_id, data):
    return None

@app.task(name="rest_hook")
def rest_hook(job_id):
    job = Job.objects.get(id=job_id)
    job.progress["current"] += 1
    job.save()

    try:
        requests.post(job.callback, data={
            'job_id': job.id,
            'progress': json.dumps(job.progress),
        })
    except requests.exceptions.InvalidSchema as e:
        print(e)
    except requests.exceptions.ConnectionError as e:
        print(e)

    if job.progress["total"] <= 0 or (job.progress["total"] > 0 and job.progress["current"] == job.progress["total"]):
        job.delete()
