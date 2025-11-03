from rest_framework import serializers
from .models import SpyObject


class DistributionSerializer(serializers.ModelSerializer):
    distribution_parameters = serializers.JSONField()
    class Meta:
        model = SpyObject
        fields = [
            'id',
            'user',
            'name',
            'description',
            "lower_threshold",
            "upper_threshold",
            "category",
            "alert_flag",
            'created_at',
            'updated_at'
        ]
        read_only_fields = ['id', 'user', 'created_at', 'updated_at']
