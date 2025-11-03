from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, HTTPException
import time

class RateLimiter(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self.requests = defaultdict(list)
        self.max_requests = 100
        self.window = 3600
    
    async def dispatch(self, request: Request, call_next):
        client_ip = request.client.host if request.client else "unknown"
        
        
        if request.url.path in ['/docs', '/redoc', '/openapi.json', '/health']:
            return await call_next(request)
        
        
        if self._is_rate_limited(client_ip):
            raise HTTPException(
                status_code=429, 
                detail="Rate limit exceeded"
            )
        
        return await call_next(request)
    
    def _is_rate_limited(self, client_ip: str) -> bool:
        """Проверяет не превысил ли клиент лимит запросов"""
        now = time.time()
        client_requests = self.requests[client_ip]

        client_requests[:] = [req_time for req_time in client_requests 
                            if now - req_time < self.window]

        if len(client_requests) >= self.max_requests:
            return True

        client_requests.append(now)
        return False