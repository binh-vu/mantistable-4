from rest_framework import serializers
from api.models import Job


class JobSerializer(serializers.ModelSerializer):
    tables = serializers.SerializerMethodField()
    progress = serializers.JSONField()

    class Meta:
        model = Job
        fields = ('id', 'created', 'tables', 'progress', 'callback')

    def get_tables(self, obj):
        return len(obj.tables)


class MantisHookSerializer(serializers.Serializer):
    job_id = serializers.IntegerField(default=-1)
    table_id = serializers.IntegerField(default=-1)
    header = serializers.CharField(max_length=200)
    payload = serializers.JSONField(default={})