from django.urls import path, include
from . import views

from dashboard.api import urls as dashboard_api_urls

urlpatterns = [
    path('', views.HomeView.as_view(), name='home'),
    path('login/', views.LoginView.as_view(), name='login'),
    path('service/', views.ServiceView.as_view(), name='service'),
    path('debug-logs/', views.DebugLogsView.as_view(), name='logs'),
    path('process/', views.ProcessView.as_view(), name='process'),
    path('export/', views.ExportView.as_view(), name='export'),
    path('dashboard-api/', include(dashboard_api_urls)),
]
