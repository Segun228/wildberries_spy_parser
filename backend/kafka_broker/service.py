from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import logging
import uuid
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

KAFKA_BROKER_DOCKER = os.getenv("KAFKA_BROKER_DOCKER")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
PRODUCER_CLIENT_ID = os.getenv("PRODUCER_CLIENT_ID")

SERVICE_TOPICS = {
    "KAFKA_TG_SEND_TOPIC":None,
    "KAFKA_EMAIL_SEND_TOPIC":None
}

LOGS = os.getenv("LOGS")
if not LOGS or LOGS.lower() in ("0", "false", "no", "nan", "", "n", "f"):
    LOGS = False
else:
    LOGS = True

def ensure_topics_exists():
    load_dotenv()
    try:
        for TOPIC_TYPE, KAFKA_TOPIC in SERVICE_TOPICS.items():
            KAFKA_TOPIC = os.getenv(TOPIC_TYPE)
            if KAFKA_TOPIC is None or not KAFKA_TOPIC:
                raise Exception(f"Topic {TOPIC_TYPE} was not found in env")
            for i in range(10):
                try:
                    admin_client = AdminClient({
                        'bootstrap.servers': KAFKA_BROKER_DOCKER
                    })

                    topic = NewTopic(
                        KAFKA_TOPIC,
                        num_partitions=1,
                        replication_factor=1
                    )

                    fs = admin_client.create_topics([topic])
                    
                    for topic_name, f in fs.items():
                        try:
                            f.result()
                            logging.info(f"Kafka topic '{topic_name}' created")
                            return
                        except Exception as e:
                            if "already exists" in str(e):
                                logging.info(f"Kafka topic '{topic_name}' already exists")
                                return
                            else:
                                raise e

                except Exception as e:
                    error_str = str(e)
                    if "Broker transport failure" in error_str or "No brokers available" in error_str:
                        logging.warning(f"[{i+1}/10] Kafka broker not available, retrying in 3s...")
                        time.sleep(3)
                    else:
                        logging.error(f"Unexpected error creating topic: {e}")
                        break
    except Exception as e:
        logging.error(e)
        raise

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        try:
            _producer = Producer({
                'bootstrap.servers': KAFKA_BROKER_DOCKER,
                'client.id': PRODUCER_CLIENT_ID,
                'message.timeout.ms': 5000,
                'retries': 3
            })
        except Exception as e:
            logging.warning(f"Kafka producer not available: {e}")
            _producer = None
    return _producer

def delivery_report(err, msg):
    if err is not None:
        logging.error(f'Message delivery failed: {err}')
    else:
        logging.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if LOGS:
    ensure_topics_exists()


