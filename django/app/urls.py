from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('home/', views.home, name='home'),
    path('job-handler/', views.JobHandler.as_view(), name='job-handler')
]
