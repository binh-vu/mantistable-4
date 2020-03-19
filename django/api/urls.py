from django.urls import path, re_path
from . import views

urlpatterns = [
    path('job/', views.JobView.as_view(), name="api_job"),
]
