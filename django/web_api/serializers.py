from rest_framework import serializers
from dashboard.models import Table


class TableSerializer(serializers.ModelSerializer):
    class Meta:
        model = Table
        fields = ('name', 'dataset', 'original', 'rows_count', 'cols_count', 'has_annotations')