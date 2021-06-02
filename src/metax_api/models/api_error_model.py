class ApiErrorModel(models.Model):

    d = models.BigAutoField(primary_key=True, editable=False)
    identifier = models.CharField(max_length=200, unique=True, null=False)
    error = JSONField(null=False)
    date_created = models.DateTimeField()