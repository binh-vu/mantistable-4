from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from api.models import Job
from web.models import Table, Dataset

from celery import current_app

import json
import requests

class JobView(View):
    def get(self, request):
        jobs = Job.objects.all()

        return JsonResponse([
            self._serialize_job(job)
            for job in jobs
        ], safe=False)

    def _serialize_job(self, job):
        return {
            "id": job.id,
            "created": job.created,
            "tables": len(job.tables),
            "progress": job.progress,
            "callback": job.callback
        }


class DatasetView(View):
    def get(self, request):
        offset_filter = int(request.GET.get("offset", "0"))
        limit_filter = int(request.GET.get("limit", "0"))
        sort_filter = request.GET.get("sort", "name")
        order_filter = {
            "asc": "",
            "desc": "-"
        }.get(request.GET.get("order", "asc"))

        datasets = Dataset.objects.all()
        datasets_count = datasets.count()
        datasets = datasets.order_by(order_filter + sort_filter)

        if limit_filter > 0:
            datasets = datasets[offset_filter:offset_filter+limit_filter]
        else:
            datasets = datasets[offset_filter:]

        return JsonResponse({
            "total": datasets_count,
            "rows": [
                self._serialize_datasets(dataset)
                for dataset in datasets
            ]
        }, safe=False)

    def _serialize_datasets(self, dataset):
        return {
            "name": dataset.name,
            "average_rows": dataset.average_rows,
            "average_cols": dataset.average_cols,
            "table_count": dataset.table_count,
            "has_annotations": dataset.has_annotations
        }


class CeleryLoadView(View):
    def get(self, request):
        worker_name = "celery@main"

        active = 0
        reserved = 0
        inspector = current_app.control.inspect([worker_name])
        if inspector.active() is not None:
            active = len(inspector.active()[worker_name])
            reserved = len(inspector.reserved()[worker_name])

        return JsonResponse({
            "active": active,
            "reserved": reserved
        })


class MainResultView(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(MainResultView, self).dispatch(request, *args, **kwargs)

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        data = json.loads(request.body)

        job_id = data.get("job_id", -1)
        table_id = data.get("table_id", -1)
        header = data.get("header", "invalid")
        payload = data.get("payload", "invalid")

        if job_id < 0:
            return JsonResponse({"status": "bad format"}, safe=False)
        
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(job_id, table_id, header)

        try:
            table = Table.objects.get(id=table_id)
        except Table.DoesNotExist:
            table = None

        # TODO: Implement better websocket wrapper
        requests.post("http://mantistable4_tornado:5000", json={
            "channel": "main",
            "payload": {
                "command": "UPDATE",    # TODO: Making this up just for test
                "resource": "progress",
                "payload": {}
            }
        })

        if header == "column analysis":
            # TODO: Parse format
            if table is not None:
                table.cols = payload
                table.save()            
        elif header == "computation":
            cols = []
            for row in payload:
                subject = row[0]
                linkages = []
                for link in row[1]:
                    if link[0] is not None:
                        l = (link[0][1], link[0][2], round(link[1], 2))
                        linkages.append({
                            "subject": subject,
                            "predicate": l[0],
                            "object": l[1],
                            "confidence": l[2]
                        })
                    else:
                        linkages.append({
                            "subject": subject,
                            "predicate": None,
                            "object": None,
                            "confidence": 0.0
                        })
                cols.append(linkages)

            print(cols)
            if table is not None:
                table.linkages = cols
                table.has_annotations = True
                table.save()

                # TODO: Implement better websocket wrapper
                requests.post("http://mantistable4_tornado:5000", json={
                    "channel": "main",
                    "payload": {
                        "command": "UPDATE",    # TODO: Making this up just for test
                        "resource": "datasets",
                        "payload": {}
                    }
                })
        elif header == "debug":
            requests.post("http://mantistable4_tornado:5000", json={
                "channel": "0",
                "payload": {
                    "command": "DEBUG",
                    "payload": payload
                }
            })
        else:
            print(payload)
        print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

        return JsonResponse({"status": "ack"}, safe=False)

class SearchResultView(View):
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(SearchResultView, self).dispatch(request, *args, **kwargs)

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        data = json.loads(request.body)

        job_id = data.get("job_id", -1)
        header = data.get("header", "invalid")
        payload = data.get("payload", "invalid")

        if job_id < 0:
            return JsonResponse({"status": "bad format"}, safe=False)

        if header == "computation":
            cols = []
            for row in payload:
                subject = row[0]
                linkages = []
                for link in row[1]:
                    if link[0] is not None:
                        l = (link[0][1], link[0][2], round(link[1], 2))
                        linkages.append({
                            "subject": subject,
                            "predicate": l[0],
                            "object": l[1],
                            "confidence": l[2]
                        })
                    else:
                        linkages.append({
                            "subject": subject,
                            "predicate": None,
                            "object": None,
                            "confidence": 0.0
                        })
                cols = linkages

            # TODO: Implement better websocket wrapper
            requests.post("http://mantistable4_tornado:5000", json={
                "channel": "service",
                "payload": {
                    "command": "UPDATE",    # TODO: Making this up just for test
                    "resource": "results",
                    "payload": cols
                }
            })
        elif header == "debug":
            requests.post("http://mantistable4_tornado:5000", json={
                "channel": "0",
                "payload": {
                    "command": "DEBUG",
                    "payload": payload
                }
            })

        return JsonResponse({"status": "ack"}, safe=False)
