from django.db import models


class Job(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    callback = models.CharField(max_length=255, blank=False)    # TODO: use models.URLField

    class Meta:
        ordering = ['created']
