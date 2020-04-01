from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('home/', views.HomeView.as_view(), name='home'),
    path('process-all/', views.ProcessAllView.as_view(), name='process-all'),
    path('job-handler/', views.JobHandler.as_view(), name='job-handler')
]
