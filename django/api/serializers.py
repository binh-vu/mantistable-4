from rest_framework import serializers

from api.models import Job
from app.models import Table

import json

class JobSerializer(serializers.Serializer):
    tables = serializers.JSONField()
    callback = serializers.CharField(max_length=200)

"""
class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = ['id', 'created', 'table_ids', 'callback']

    def to_internal_value(self, instance):
        data = super(JobSerializer, self).to_internal_value(instance)
        data.update({
            "table_ids": json.loads(instance.get("table_ids"))
        })
        return data

    def to_representation(self, instance):
        data = super(JobSerializer, self).to_representation(instance)
        data.update({
            "table_ids": instance.table_ids
        })
        return data
"""

class TableSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = "__all__"