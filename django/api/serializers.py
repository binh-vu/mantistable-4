from rest_framework import serializers

from api.models import Job

import json

class JobListSerializer(serializers.ModelSerializer):
    progress = serializers.JSONField()
    
    class Meta:
        model = Job
        fields = ['created', 'progress', 'callback']
        
        
class JobCreateSerializer(serializers.Serializer):
    tables = serializers.JSONField()
    backend_host = serializers.CharField(max_length=200)
    backend_port = serializers.IntegerField()
    backend_token = serializers.CharField(max_length=200)
    callback = serializers.CharField(max_length=200)
    debug = serializers.BooleanField(default=False)
    
    def get_backend(self):
        return {
            "host": self.data["backend_host"],
            "port": self.data["backend_port"],
            "accessToken": self.data["backend_token"]
        }
