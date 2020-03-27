from django.db import models
from djongo.models.json import JSONField

class Job(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    progress = JSONField(default={})
    callback = models.CharField(max_length=255, blank=False)    # TODO: use models.URLField

    class Meta:
        ordering = ['created']
