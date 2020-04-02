from mantistable.celery import app
from api.models import Job
from app.models import Table

import requests
import json

@app.task(name="test_task")
def test_task(job_id):
    job = Job.objects.get(id=job_id)
    tables = Table.objects.filter(id__in=job.table_ids)

    data = [
        {
            "id": table.id,
            "name": table.name,
            "rows": table.rows
        }
        for table in tables
    ]

    for i in range(0, 10000):
        for j in range(0, 10000):
            pass

    print(data)

    return job_id

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
