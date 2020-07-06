import django

class DashboardConfig(django.apps.AppConfig):
    name = 'dashboard'

    def ready(self):
        import dashboard.signals