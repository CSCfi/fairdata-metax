from django.db import models

class File(models.Model):

    id = models.UUIDField(max_length=32, primary_key=True)
    file_name = models.CharField(max_length=64, null=True)

    def __str__(self):
        return str(self.id)
