from rest_framework import serializers

from api.models import Job
from app.models import Table

class JobSerializer(serializers.ModelSerializer):
    class Meta:
        model = Job
        fields = ['id', 'created', 'table_ids', 'callback']

    def to_representation(self, instance):
        data = super(JobSerializer, self).to_representation(instance)
        data.update({
            "table_ids": instance.table_ids
        })
        return data

class TableSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = "__all__"