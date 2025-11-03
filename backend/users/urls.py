from django.urls import path, include
from .views import UserListCreateView, UserRetrieveUpdateDestroyView, GetActiveUsers
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.views import TokenRefreshView


urlpatterns = [
    path("user/active/", GetActiveUsers.as_view(), name="active-user-list-create-endpoint"),

    path("user/", UserListCreateView.as_view(), name="user-list-create-endpoint"),
    path("user/<int:id>/", UserRetrieveUpdateDestroyView.as_view(), name="user-retrieve-update-destroy-endpoint"),

    

    path("api/token/", TokenObtainPairView.as_view(), name="get_token"),
    path("api/token/refresh/", TokenRefreshView.as_view(), name="refresh_token"),
    path("api-auth/", include("rest_framework.urls")),
]