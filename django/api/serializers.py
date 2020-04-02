from rest_framework import serializers
from api.models import Job

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