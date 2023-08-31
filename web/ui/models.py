from django.db import models

# Create your models here.
class Tests(models.Model):
    run_time = models.CharField(max_length=45)
    log_file = models.CharField(max_length=45)
    db_file =  models.CharField(max_length=45)