from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics

from api.serializers import JobSerializer
from api.models import Job
import api.tasks as tasks

class JobView(generics.ListCreateAPIView):
    queryset = Job.objects.all()
    serializer_class = JobSerializer
    
    """ TODO: useful to hide callback field in get request
    def get_serializer_class(self):
        if self.request.method == 'POST':
            return WriteJobSerializer
        return ReadJobSerializer
    """
    
    def perform_create(self, serializer):
        job = serializer.save()
        job.progress = {
            "current": 0,
            "total": 1
        }
        job.save()
        
        # Call celery here
        tasks.test_task.apply_async(
            args=(5, job.id),
            link=[tasks.rest_hook.s()]
        )