from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import jwt
from typing import Callable

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        public_paths = ['/docs', '/redoc', '/openapi.json', '/health']
        if any(request.url.path.startswith(path) for path in public_paths):
            return await call_next(request)
        
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return Response(
                content='{"detail": "Missing or invalid token"}',
                status_code=401,
                media_type="application/json"
            )
        
        token = auth_header.replace('Bearer ', '')
        
        try:
            payload = jwt.decode(token, "secret-key", algorithms=["HS256"])
            request.state.user = payload
        except jwt.ExpiredSignatureError:
            return Response(
                content='{"detail": "Token expired"}',
                status_code=401,
                media_type="application/json"
            )
        except jwt.InvalidTokenError:
            return Response(
                content='{"detail": "Invalid token"}',
                status_code=401,
                media_type="application/json"
            )
        
        return await call_next(request)