from rest_framework import generics

from dashboard.models import Table
from web_api.serializers import TableSerializer


class TableView(generics.ListAPIView):
    serializer_class = TableSerializer
    model = serializer_class.Meta.model
    queryset = model.objects.order_by('name')
    