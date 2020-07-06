from django.views import View
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework import generics
from rest_framework.response import Response

from api.models import Job
from dashboard.models import Table, Dataset
from dashboard.api.serializers import JobSerializer, MantisHookSerializer

from celery import current_app

import json
import requests
import mantistable.settings as settings

class LamapiView(generics.GenericAPIView):
    def get(self, request):
        backends = settings.LAMAPI_BACKENDS

        json_response = []
        for backend_id in backends:
            lamapi = backends[backend_id]
            host = lamapi["host"]
            port = lamapi["port"]
            description = "-"
            prefixes = []
            status = "ONLINE"

            try:
                # TODO: terrible...
                result = requests.get("http://" + host + ":" + str(port) + "/infos", timeout=2)
                if result.status_code != 200:
                    status = "OFFLINE"
                else:
                    description = result.json()["description"]
                    prefixes = result.json()["prefixes"]
            except:
                status = "OFFLINE"

            lamapi_info = {
                "id": backend_id,
                "host": host,
                "port": port,
                "description": description,
                "prefixes": prefixes,
                "status": status,
            }
            json_response.append(lamapi_info)


        return Response(json_response)

# TODO: Should not use this becouse job model should not be referenced in this app
class JobView(generics.ListAPIView):
    serializer_class = JobSerializer
    model = serializer_class.Meta.model
    queryset = model.objects.order_by('created')
    pagination_class = None
    

class DatasetView(generics.GenericAPIView):
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
            #"has_annotations": dataset.has_annotations
        }


class CeleryLoadView(generics.GenericAPIView):
    def get(self, request):
        worker_name = "celery@main"

        active = 0
        reserved = 0
        inspector = current_app.control.inspect([worker_name])
        if inspector.active() is not None:
            active = len(inspector.active()[worker_name])
            reserved = len(inspector.reserved()[worker_name])

        return Response({
            "active": active,
            "reserved": reserved
        })


class MainResultView(generics.GenericAPIView):
    serializer_class=MantisHookSerializer
    """
    @method_decorator(csrf_exempt)
    def dispatch(self, request, *args, **kwargs):
        return super(MainResultView, self).dispatch(request, *args, **kwargs)
    """

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            job_id = serializer.data["job_id"]
            table_id = serializer.data["table_id"]
            header = serializer.data["header"]
            payload = serializer.data["payload"]
        
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

                #print(cols)
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
                #print(payload)
                pass
            print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

            return Response({"status": "ack"})

        return Response({"status": "invalid"})


class SearchResultView(generics.GenericAPIView):
    serializer_class=MantisHookSerializer

    # TODO: In general this is a bit dangerous if someone other than api post on this
    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        if serializer.is_valid():
            job_id = serializer.data["job_id"]
            header = serializer.data["header"]
            payload = serializer.data["payload"]

            if header == "computation":
                cols = []
                for row in payload:
                    subject = row[0]
                    confidence = round(row[2], 2)
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
                    cols = {
                        "linkages": linkages,
                        "confidence": confidence / len(linkages)
                    }

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

            return Response({"status": "ack"})

        return Response({"status": "invalid"})
