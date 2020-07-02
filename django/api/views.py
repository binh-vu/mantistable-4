from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics
from rest_framework.exceptions import ValidationError

from api.process.utils.mongo.repository import Repository
from api.serializers import JobSerializer
from api.models import Job
import api.tasks as tasks
from api.process.utils.lamapi import LamAPIWrapper

import requests
import json

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    
    def perform_create(self, serializer):
        backend = serializer.get_backend()
        self._check_backend(backend)
        try:
            tables = json.loads(serializer.data["tables"])
        except:
            raise ValidationError('Invalid json body')

        job = Job(
            tables=tables,
            backend=backend,
            callback=serializer.data["callback"],
            progress={
                "current": 0,
                "total": 3
            }
        )
        job.save()

        job.task_id = tasks.job_slot(job.id)
        job.save()

    def _check_backend(self, backend):
        lamapi = LamAPIWrapper(backend["host"], backend["port"], backend["accessToken"])
        try:
            lamapi.infos()
        except:
            raise ValidationError('Backend is unreachable')
