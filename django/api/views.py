from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics

from api.serializers import JobSerializer
from api.models import Job
import api.tasks as tasks

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    
    def perform_create(self, serializer):
        job = serializer.save()
        job.progress = {
            "current": 0,
            "total": 1
        }
        job.save()
        
        # Call celery here
        tasks.test_task.apply_async(
            args=(job.id,),
            link=[tasks.rest_hook.s()]
        )