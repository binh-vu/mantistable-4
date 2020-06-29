from rest_framework import generics
import mantistable.settings as settings

from dashboard.models import Table
from web_api.serializers import TableSerializer


class TableView(generics.ListAPIView):
    serializer_class = TableSerializer
    model = serializer_class.Meta.model
    queryset = model.objects.order_by('name')
    #pagination_class = settings.REST_FRAMEWORK["DEFAULT_PAGINATION_CLASS"]