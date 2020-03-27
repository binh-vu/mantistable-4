from mantistable.celery import app
from api.models import Job

import requests
import json

@app.task(name="test_task")
def test_task(value, job_id):
    for i in range(0, 10000):
        for j in range(0, 10000):
            value = i

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
