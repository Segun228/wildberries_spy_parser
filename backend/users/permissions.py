import os

from rest_framework import permissions
from dotenv import load_dotenv
from rest_framework.permissions import IsAdminUser

load_dotenv()


admin_one = os.getenv("ADMIN_1")
admin_two = os.getenv("ADMIN_2")
debug = os.getenv("DEBUG", "False").lower() in ("true", "t", "y", "yes", "1")

admins = [admin_one, admin_two]

class IsAdminOrDebugOrReadOnly(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        if request.method in permissions.SAFE_METHODS:
            return True
        admin_checker = IsAdminUser()
        if admin_checker.has_object_permission(request=request, view=view, obj=obj):
            return True
        auth = request.headers.get("Authorization")
        if not auth:
            return False
        header = auth.split(" ")
        if header[0] != "Bot":
            return False
        if header[1] in admins:
            return True