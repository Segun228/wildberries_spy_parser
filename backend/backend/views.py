from rest_framework.views import APIView
from rest_framework.response import Response


class HealthCheckView(APIView):
    def post(self, request, *args, **kwargs):
        return Response(
            data = {
                "status":"ok",
                "info":"the main backend service is healthy"
            },
            status=200
        )
    def get(self, request, *args, **kwargs):
        return Response(
            data = {
                "status":"ok",
                "info":"the main backend service is healthy"
            },
            status=200
        )
    def put(self, request, *args, **kwargs):
        return Response(
            data = {
                "status":"ok",
                "info":"the main backend service is healthy"
            },
            status=200
        )
    def delete(self, request, *args, **kwargs):
        return Response(
            data = {
                "status":"ok",
                "info":"the main backend service is healthy"
            },
            status=200
        )
    