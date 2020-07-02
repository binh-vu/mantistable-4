from django.urls import path
from . import views

urlpatterns = [
    path('tables/', views.TableView.as_view(), name='tables'),
    path('datasets/', views.DatasetView.as_view(), name='datasets'),
]
