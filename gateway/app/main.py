from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import httpx
import time
from .settings import settings
from .middleware.auth import AuthMiddleware
from .middleware.logs import LoggingMiddleware
from .middleware.rate_limiter import RateLimiter
app = FastAPI(title="API Gateway")

# app.add_middleware(RateLimiter)
# app.add_middleware(LoggingMiddleware)
# app.add_middleware(AuthMiddleware)


SERVICES = {
    "users": {
        "base_url": "http://user-service:8001",
        "routes": ["/users", "/auth", "/profile"]
    },
    "orders": {
        "base_url": "http://order-service:8002", 
        "routes": ["/orders", "/cart"]
    },
    "products": {
        "base_url": "http://product-service:8003",
        "routes": ["/products", "/categories"]
    }
}

@app.get("/health")
async def health_ping():
    return {"status": "ok", "service": "parser"}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_request(path: str, request: Request):
    """Основной прокси-роут"""
    start_time = time.time()
    target_service = find_target_service(path, request.method)
    if not target_service:
        raise HTTPException(status_code=404, detail="Service not found")
    
    target_url = f"{target_service['base_url']}/{path}"
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.request(
                method=request.method,
                url=target_url,
                headers=filter_headers(dict(request.headers)),
                content=await request.body()
            )
            
            
            duration = time.time() - start_time
            await log_metrics(path, response.status_code, duration)
            
            
            return JSONResponse(
                content=response.json() if response.content else None,
                status_code=response.status_code,
                headers=dict(response.headers)
            )
            
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Service unavailable")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Service timeout")

def find_target_service(path: str, method: str) -> dict|None:
    """Находит целевой сервис по пути запроса"""
    for service_name, service_config in SERVICES.items():
        for route in service_config["routes"]:
            if path.startswith(route.lstrip('/')):
                return service_config
    return None

def filter_headers(headers: dict) -> dict:
    """Фильтрует заголовки для передачи в сервисы"""
    filtered = {}
    for key, value in headers.items():
        if key.lower() not in ['host', 'content-length']:
            filtered[key] = value
    return filtered


async def log_metrics(path: str, status_code: int, duration: float):
    """Логирование метрик"""
    print(f"Metrics: {path} | Status: {status_code} | Duration: {duration:.3f}s")


