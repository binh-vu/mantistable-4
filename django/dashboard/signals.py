from django.dispatch import receiver
from django.db.models.signals import post_migrate
from django.contrib.auth.models import User

@receiver(post_migrate)
def on_init_db(**kwargs):
    def _create_superuser():
        has_superuser = User.objects.filter(is_superuser=True).count() > 0

        if not has_superuser:
            print("Creating default admin")
            admin = User.objects.create_superuser('admin', email='', password='mantis4')
            admin.save()
    
    _create_superuser()
    post_migrate.disconnect(on_init_db)