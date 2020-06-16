from django.urls import path
from . import views

urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path('service/', views.ServiceView.as_view(), name='service'),
    path('process/', views.ProcessView.as_view(), name='process'),
    path('export/', views.ExportView.as_view(), name='export'),
]
