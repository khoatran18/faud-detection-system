import json
import logging
import socket
import random
from confluent_kafka import Producer

from config.settings import Settings

logger = logging.getLogger(__name__)

class KafkaProducer:
    """
    Kafka producer wrapper.
    """

    def __init__(self, settings: Settings):
        self.settings = settings

        conf = {
            'bootstrap.servers': settings.kafka.server.bootstrap_servers,
            'client.id': socket.gethostname(),
            'retries': settings.kafka.producer.retries
        }

        logger.info("Kafka Producer config: %s", conf)
        self.producer = Producer(conf)
        logger.info("Kafka Producer initialized")

    def _acked(self, err, msg):
        if err is not None:
            logger.error("Failed to deliver message %s with error %s", str(msg), err)
        else:
            logger.info("Message delivered successfully to %s, partition %s, offset %d", msg.topic(), msg.partition(), msg.offset())


    def send(self, topic, value):
        """
        Send records to kafka topic.
        """

        try:
            payload = json.dumps(value, ensure_ascii=False).encode('utf-8')
            key = str(random.randint(0, 10)).encode('utf-8')

            self.producer.produce(
                topic=topic,
                value=payload,
                callback=self._acked,
                key=key
            )
            self.producer.poll(0)
        except BufferError:
            logger.error("Kafka buffer is full, waiting")
            self.producer.poll(1)
        except Exception as e:
            logger.error("Error when send message to kafka: %s", e)

    def flush(self):
        """
        Flush all records in buffer
        """

        logger.info("Flushing kafka producer buffer")
        self.producer.flush()

    def close(self):
        """
        Close kafka producer
        """

        self.producer.flush()
        logger.info("Closed kafka producer")