from django.db import models
from django.dispatch import receiver
from django.core.files.uploadedfile import SimpleUploadedFile
from djongo.models.json import JSONField

import json
import os
import mantistable.settings as settings
from datetime import timedelta


class Locations:
    @staticmethod
    def get_upload_path(instance, filename):
        return filename


class Job(models.Model):
    class Meta:
        ordering = ['created']

    task_id = models.CharField(max_length=765, blank=False)
    created = models.DateTimeField(auto_now_add=True)
    #tables = JSONField(default=[])

    _tables = models.FileField(upload_to=Locations.get_upload_path)

    progress = JSONField(default={})
    backend = JSONField(default={})
    callback = models.CharField(max_length=255, blank=False)    # TODO: use models.URLField

    @property
    def tables(self):
        if self._tables:
            data = json.loads(self._tables.read())
            self._tables.seek(0)
            return data

        return []

    @tables.setter
    def tables(self, value):
        self._tables = SimpleUploadedFile("upload", json.dumps(value).encode())

# SIGNALS ===================================================

@receiver(models.signals.post_delete, sender=Job)
def remove_file_on_delete_gs(sender, instance, **kwargs):
    tables = instance._tables
    if tables:
        path = os.path.join(settings.MEDIA_ROOT, tables.path)
        if os.path.isfile(path):
            os.remove(path)


@receiver(models.signals.pre_save, sender=Job)
def remove_file_on_change_gs(sender, instance, **kwargs):
    if not instance.pk:
        return False

    try:
        job = Job.objects.get(id=instance.id)
    except Job.DoesNotExist:
        return False

    new_job, old_job = job._tables, instance._tables
    if not old_job == new_job:
        path = os.path.join(settings.MEDIA_ROOT, old_job.path)
        if os.path.isfile(path):
            os.remove(path)

    return True
