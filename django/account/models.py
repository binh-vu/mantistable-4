from django.db import models
from django.contrib.auth.models import User

import os

def get_user_location(instance, filename):
    return os.path.join(instance.user.username, "avatar")

class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    avatar = models.ImageField(upload_to=get_user_location, default='default_avatar.png')
