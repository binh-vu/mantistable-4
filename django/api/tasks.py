from mantistable.celery import app
from api.models import Job

import requests

@app.task(name="test_task")
def test_task(value, job_id):
    for i in range(0, 10000):
        for j in range(0, 10000):
            value = i

    return job_id

@app.task(name="rest_hook")
def rest_hook(job_id):
    job = Job.objects.get(id=job_id)
    callback = job.callback

    print(callback)
    try:
        r = requests.post(callback, data={
            'number': 12524,
            'type': 'issue',
            'action': 'test'
        })
        print(r.status_code, r.reason)
    except requests.exceptions.InvalidSchema as e:
        print(e)
    except requests.exceptions.ConnectionError as e:
        print(e)

    job.delete()
