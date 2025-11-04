from rest_framework.generics import ListCreateAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.views import APIView

from .models import SpyBrand, SpyObject
from .serializers import SpyBrandSerializer, SpyObjectSerializer

from rest_framework.permissions import AllowAny, IsAuthenticated

from users.permissions import IsAdminOrDebugOrReadOnly

from backend.authentication import CombinedAuthentication


from rest_framework.exceptions import ValidationError
from django.shortcuts import get_object_or_404
import logging

from kafka_broker.utils import build_log_message

from rest_framework.response import Response
from rest_framework import mixins, generics

from django.core.cache import cache
from dotenv import load_dotenv

from django.http import HttpResponseBadRequest

import os
import requests
import pandas as pd
import uuid
from io import BytesIO
load_dotenv()



class AuthenticatedAPIView:
    authentication_classes = [CombinedAuthentication]
    permission_classes = [IsAuthenticated]



class SpyBrandListCreateAPIView(ListCreateAPIView):
    authentication_classes = [CombinedAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = SpyBrandSerializer

    def get_queryset(self):
        return SpyBrand.objects.filter(user=self.request.user)



class SpyBrandRetrieveUpdateDestroyAPIView(ListCreateAPIView):
    authentication_classes = [CombinedAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = SpyBrandSerializer

    lookup_field = 'id'
    lookup_url_kwarg = 'spy_brand_id'

    def get_queryset(self):
        return SpyBrand.objects.filter(user=self.request.user)



class SpyObjectListCreateAPIView(ListCreateAPIView):
    authentication_classes = [CombinedAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = SpyObjectSerializer

    lookup_field = 'id'
    lookup_url_kwarg = 'spy_object_id'

    def get_queryset(self):
        return SpyObject.objects.filter(user=self.request.user)



class SpyObjectRetrieveUpdateDestriyAPIView(ListCreateAPIView):
    authentication_classes = [CombinedAuthentication]
    permission_classes = [IsAuthenticated]
    serializer_class = SpyObjectSerializer

    lookup_field = 'id'
    lookup_url_kwarg = 'spy_object_id'

    def get_queryset(self):
        return SpyObject.objects.filter(user=self.request.user)
