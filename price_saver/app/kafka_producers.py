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
topic_list = [
    "KAFKA_SAVE_PRICE_TOPIC"
]

KAFKA_BROKER_DOCKER = os.getenv("KAFKA_BROKER_DOCKER")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")
KAFKA_ = os.getenv("KAFKA_BACKEND_LOGS_TOPIC")
PRODUCER_CLIENT_ID = os.getenv("PRODUCER_CLIENT_ID")

topic_dict = {}

for topic in topic_list:
    env_topic = os.getenv(topic)
    if not env_topic:
        raise KeyError(f"Error while getting topic {topic} from .env")
    topic_dict[topic] = env_topic

LOGS = os.getenv("LOGS")
if not LOGS or LOGS.lower() in ("0", "false", "no", "nan", "", "n", "f"):
    LOGS = False
else:
    LOGS = True

def ensure_topic_exists(KAFKA_TOPIC):
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

def ensure_all_topics_exist():
    try:
        for ind, (key, el) in enumerate(topic_dict.items()):
            ensure_topic_exists(
                KAFKA_TOPIC=el
            )
    except Exception as e:
        logging.error("Error while creating topics:")
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


def send_to_kafka(data, KAFKA_TOPIC):
    if not LOGS:
        logging.warning("The LOGS mode is turned off or the variable is not set")
        return {"status": "skipped", "reason": "logs_disabled"}
        
    producer = get_producer()
    if producer is None:
        logging.info(f"Kafka not available, skipping log")
        return {"status": "skipped", "reason": "producer_unavailable"}

    try:
        messages = []

        if isinstance(data, list):
            for item in data:
                if hasattr(item, "dict"):
                    item = item.dict()
                messages.append(item)
                producer.produce(
                    KAFKA_TOPIC,
                    json.dumps(item).encode('utf-8'),
                    callback=delivery_report
                )
        else:
            if hasattr(data, "dict"):
                data = data.dict()
            messages.append(data)
            producer.produce(
                KAFKA_TOPIC,
                json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )

        producer.flush(timeout=5)
        
        return {
            "status": "success",
            "messages_sent": len(messages),
            "sample": messages[0] if messages else None,
        }

    except Exception as e:
        logging.error(f"Failed to send to Kafka: {e}")
        return {"status": "failed", "error": str(e)}



async def send_parsing_result(
    data:dict,
):
    pass
    #TODO