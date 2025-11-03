from django.apps import AppConfig
import logging
import os
from dotenv import load_dotenv

class MyAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = "backend"
    def ready(self):
        load_dotenv()
        LOGS = os.getenv("LOGS")
        if not LOGS or LOGS.lower() in ("0", "false", "no", "nan", ""):
            LOGS = False
        from signals import signals
        try:
            from kafka_broker.utils import ensure_topic_exists
            from kafka_broker.service import ensure_topics_exists
            if LOGS:
                ensure_topic_exists()
        except Exception as e:
            logging.warning(f"Kafka topic creation skipped: {e}")