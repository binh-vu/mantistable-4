from rest_framework import serializers

from api.models import Job

import json

class JobSerializer(serializers.Serializer):
    tables = serializers.JSONField()
    backend_host = serializers.CharField(max_length=200)
    backend_port = serializers.IntegerField()
    backend_token = serializers.CharField(max_length=200)
    callback = serializers.CharField(max_length=200)

    """
    def validate_backend_host(self, value):
       if not value.startswith("http"):
           return "http://" + value

       return value

    def validate_callback(self, value):
       if not value.startswith("http"):
           return "http://" + value

       return value
    """
    
    def get_backend(self):
        return {
            "host": self.data["backend_host"],
            "port": self.data["backend_port"],
            "accessToken": self.data["backend_token"]
        }
