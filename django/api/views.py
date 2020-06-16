from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics

from api.process.utils.mongo.repository import Repository
from api.serializers import JobSerializer
from api.models import Job
import api.tasks as tasks

from web.models import Table

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    
    def perform_create(self, serializer):
        # TODO: sanitize input

        job = Job(
            tables=serializer.data["tables"],
            callback=serializer.data["callback"],
            progress={
                "current": 0,
                "total": 3
            }
        )
        job.save()

        job.task_id = tasks.job_slot(job.id)
        job.save()