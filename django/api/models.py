from django.db import models
from djongo.models.json import JSONField
from datetime import timedelta

class Job(models.Model):
    task_id = models.CharField(max_length=765, blank=False)
    created = models.DateTimeField(auto_now_add=True)
    #table_ids = JSONField(default=[]) 
    tables = JSONField(default=[])
    progress = JSONField(default={})
    backend = JSONField(default={})
    callback = models.CharField(max_length=255, blank=False)    # TODO: use models.URLField
    #callback = models.URLField(max_length=255, blank=False)    # TODO: use models.URLField

    class Meta:
        ordering = ['created']
