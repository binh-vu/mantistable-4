from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('home/', views.HomeView.as_view(), name='home'),
    path('service/', views.ServiceView.as_view(), name='service'),
    path('process/', views.ProcessView.as_view(), name='process'),
    path('jobs/', views.JobView.as_view(), name='jobs'),
    path('datasets/', views.DatasetView.as_view(), name='datasets'),
    path('job-handler/', views.JobHandler.as_view(), name='job-handler'),
    path('celery-stats/', views.CeleryLoadView.as_view(), name='celery-stats'),

    path('search-result/', views.SearchResultView.as_view(), name="search-result")
]
