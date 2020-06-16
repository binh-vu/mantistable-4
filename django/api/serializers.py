from rest_framework import serializers

from api.models import Job
from web.models import Table

import json

class JobSerializer(serializers.Serializer):
    tables = serializers.JSONField()
    callback = serializers.CharField(max_length=200)

class TableSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = "__all__"