from django.db import models


class Job(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    callback = models.URLField(blank=False)

    class Meta:
        ordering = ['created']
