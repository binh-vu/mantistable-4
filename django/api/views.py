from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics, viewsets
from rest_framework.exceptions import ValidationError

from api.process.utils.mongo.repository import Repository
from api.serializers import JobListSerializer, JobCreateSerializer
from api.models import Job
import api.tasks as tasks
from api.process.utils.lamapi import LamAPIWrapper

import requests
import json

"""
class JobView(generics.ListAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
"""

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    #serializer_class = JobListSerializer
    
    def get_serializer_class(self):
        if self.request.method == 'POST':
            return JobCreateSerializer
        
        return JobListSerializer
    
    def perform_create(self, serializer):
        print("Creating job")
        backend = serializer.get_backend()
        self._check_backend(backend)

        job = Job(
            tables=serializer.data["tables"],
            backend=backend,
            callback=serializer.data["callback"],
            progress={
                "current": 0,
                "total": 3
            }
        )
        job.save()

        print("Going into job slot!")

        job.task_id = tasks.job_slot(job.id)
        job.save()

    def _check_backend(self, backend):
        lamapi = LamAPIWrapper(backend["host"], backend["port"], backend["accessToken"])
        try:
            lamapi.infos()
        except:
            raise ValidationError('Backend is unreachable')
