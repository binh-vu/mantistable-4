from rest_framework import serializers
from dashboard.models import Table, Dataset


class TableSerializer(serializers.ModelSerializer):
    original = serializers.JSONField()
    
    class Meta:
        model = Table
        fields = ('name', 'dataset', 'original', 'rows_count', 'cols_count', 'has_annotations')


class DatasetSerializer(serializers.ModelSerializer):   
    class Meta:
        model = Dataset
        fields = ('name', 'table_count', 'average_rows', 'average_cols')