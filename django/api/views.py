from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import generics

from api.serializers import JobSerializer
from api.models import Job

import requests

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
        serializer.save()
        
        # Call celery here
        
        # Debug: remove
        callback = serializer.validated_data["callback"]
        print(callback)
        r = requests.post(callback, data= {
            'number': 12524,
            'type': 'issue',
            'action': 'test'
        })
        print(r.status_code, r.reason)
