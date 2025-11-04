from django.db import models
from users.models import User
from django.contrib.postgres.fields import ArrayField


class SpyObject(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="spy_objects")
    name = models.CharField(max_length=100, null=False, blank=False, default="Товар для шпионажа")
    description = models.CharField(max_length=1000, null=True, blank=True, default="Описание распределения")
    lower_threshold = models.FloatField()
    upper_threshold = models.FloatField()
    category = ArrayField(base_field=models.CharField(max_length=100))
    alert_flag = models.BooleanField(default=True, null=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.name


class SpyBrand(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="spy_objects")
    brand_id = models.IntegerField(null=False, blank=False)
    brand_name = models.CharField(max_length=100, null=False, blank=False)
    description = models.CharField(max_length=1000, null=True, blank=True, default="Описание распределения")
    alert_flag = models.BooleanField(default=True, null=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.brand_name
