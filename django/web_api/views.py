from rest_framework import generics
import mantistable.settings as settings

from dashboard.models import Table
from web_api.serializers import TableSerializer, DatasetSerializer


class TableView(generics.ListAPIView):
    serializer_class = TableSerializer
    model = serializer_class.Meta.model
    queryset = model.objects.order_by('name')

class DatasetView(generics.ListAPIView):
    serializer_class = DatasetSerializer
    model = serializer_class.Meta.model
    queryset = model.objects.order_by('-table_count')