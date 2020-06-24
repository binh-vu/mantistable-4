from django.urls import path
from . import views

urlpatterns = [
    path('jobs/', views.JobView.as_view(), name='jobs'),
    path('lamapi/', views.LamapiView.as_view(), name='lamapi'),
    path('datasets/', views.DatasetView.as_view(), name='datasets'),
    path('celery-stats/', views.CeleryLoadView.as_view(), name='celery-stats'),

    path('main-result/', views.MainResultView.as_view(), name="main-result"),
    path('search-result/', views.SearchResultView.as_view(), name="search-result")
]
