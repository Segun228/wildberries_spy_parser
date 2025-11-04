from django.urls import path
from .views import SpyBrandListCreateAPIView, SpyBrandRetrieveUpdateDestroyAPIView, SpyObjectListCreateAPIView, SpyObjectRetrieveUpdateDestriyAPIView

urlpatterns = [
    path("spy-brand/<int:spy_brand_id>/", SpyBrandRetrieveUpdateDestroyAPIView.as_view(), name="spy brand retrieve update destroy endpoint"),
    path("spy-brand/", SpyBrandListCreateAPIView.as_view(), name="spy brand list create endpoint"),

    path("spy-object/<int:spy_object_id>", SpyObjectRetrieveUpdateDestriyAPIView.as_view(), name="spy object retrieve update destroy endpoint"),
    path("spy-object/", SpyObjectListCreateAPIView.as_view(), name="spy object list create endpoint"),
]