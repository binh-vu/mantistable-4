from django.urls import path
from . import views

urlpatterns = [
    path('jobs/', views.JobView.as_view(), name='dashboard-jobs'),
    path('lamapi/', views.LamapiView.as_view(), name='dashboard-lamapi'),
    path('datasets/', views.DatasetView.as_view(), name='dashboard-datasets'),
    path('celery-stats/', views.CeleryLoadView.as_view(), name='dashboard-celery-stats'),

    path('main-result/', views.MainResultView.as_view(), name="main-result"),
    path('search-result/', views.SearchResultView.as_view(), name="search-result")
]
