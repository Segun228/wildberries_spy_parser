from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
import asyncio
import os
import logging
from .kafka_consumer import KafkaEmailConsumer
from .parsing import handle_parsing_request
from dotenv import load_dotenv
from fastapi import Response
from prometheus_client import generate_latest, REGISTRY
from .kafka_producer import build_log_message
from .kafka_producer import ensure_topic_exists
from prometheus_client import Counter, Histogram, generate_latest, REGISTRY

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

KAFKA_PARSING_TOPIC = os.getenv("KAFKA_PARSING_TOPIC")


REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')

@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info("Postgres table ensured")

    backend_consumer = KafkaEmailConsumer(KAFKA_PARSING_TOPIC, handle_parsing_request)
    await backend_consumer.start()
    logging.info("Kafka consumers started")

    backend_task = asyncio.create_task(backend_consumer.consume_forever())
    logging.info("Kafka consumer tasks running")

    ensure_topic_exists()
    logging.info("Kafka producer started")


    try:
        yield
    finally:
        logging.info("Shutting down Kafka consumers...")
        await backend_consumer.stop()
        backend_task.cancel()
        logging.info("Kafka consumers stopped")

app = FastAPI(lifespan=lifespan)


@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    start_time = asyncio.get_event_loop().time()
    
    response = await call_next(request)
    
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    duration = asyncio.get_event_loop().time() - start_time
    REQUEST_DURATION.observe(duration)
    build_log_message(
        user_id = (await request.json()).get("id", None),
        is_authenticated = True,
        telegram_id = (await request.json()).get("telegram_id", None),
        action = "Send email",
        response_code = 200,
        request_method = request.method,
        request_body = (await request.json()).get("id", None),
        platform = "email service",
        level = "INFO",
    )
    return response

@app.get("/email/")
async def email_post_list(request: Request):
    #TODO
    build_log_message(
        user_id = (await request.json()).get("id", None),
        is_authenticated = True,
        telegram_id = (await request.json()).get("telegram_id", None),
        action = "Send email",
        response_code = 200,
        request_method = request.method,
        request_body = (await request.json()).get("id", None),
        platform = "email service",
        level = "INFO",
    )
    return Response(
        content=generate_latest(REGISTRY),
        media_type="text/plain"
    )

@app.get("/email/<int:id>/")
async def email_retrieve_update_destroy(request: Request):
    #TODO
    build_log_message(
        user_id = (await request.json()).get("id", None),
        is_authenticated = True,
        telegram_id = (await request.json()).get("telegram_id", None),
        action = "Send email",
        response_code = 200,
        request_method = request.method,
        request_body = (await request.json()).get("id", None),
        platform = "email service",
        level = "INFO",
    )
    return Response(
        content=generate_latest(REGISTRY),
        media_type="text/plain"
    )

@app.get("/")
async def ping(request: Request):
    build_log_message(
        user_id = (await request.json()).get("id", None),
        is_authenticated = True,
        telegram_id = (await request.json()).get("telegram_id", None),
        action = "Send email",
        response_code = 200,
        request_method = request.method,
        request_body = (await request.json()).get("id", None),
        platform = "email service",
        level = "INFO",
    )
    return {
        "status": "ok",
        "text":"Parser service is alive"
    }