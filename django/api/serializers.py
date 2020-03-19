from rest_framework import serializers
from api.models import Job

class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = ['id', 'created', 'callback']
