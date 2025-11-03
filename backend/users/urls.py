from django.urls import path, include
from .views import UserListCreateView, UserRetrieveUpdateDestroyView, GetActiveUsers, UserTelegramRetrieveUpdateDestroyView, UserTelegramListCreateView
from rest_framework_simplejwt.views import TokenObtainPairView
from rest_framework_simplejwt.views import TokenRefreshView


urlpatterns = [
    path("user/active/", GetActiveUsers.as_view(), name="active-user-list-create-endpoint"),

    path("user/<int:id>/", UserRetrieveUpdateDestroyView.as_view(), name="user-retrieve-update-destroy-endpoint"),
    path("user/", UserListCreateView.as_view(), name="user-list-create-endpoint"),

    path("user/telegram/<str:telegram_id>/", UserTelegramRetrieveUpdateDestroyView.as_view(), name="user-telegram-retrieve-update-destroy-endpoint"),
    path("user/telegram/", UserTelegramRetrieveUpdateDestroyView.as_view(), name="user-telegram-retrieve-update-destroy-endpoint"),

    path("auth/token/refresh/", TokenRefreshView.as_view(), name="refresh_token"),
    path("auth/token/", TokenObtainPairView.as_view(), name="get_token"),
    path("api-auth/", include("rest_framework.urls")),
]