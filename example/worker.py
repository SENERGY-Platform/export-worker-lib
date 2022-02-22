import ew_lib
import confluent_kafka
import signal
import os

# Environment variables for configuration. See 'docker-compose.yml' for more information.
METADATA_BROKER_LIST = os.getenv("METADATA_BROKER_LIST")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "export-worker-test")
FILTER_CONSUMER_GROUP_ID = os.getenv("FILTER_CONSUMER_GROUP_ID")
FILTER_TOPIC = os.getenv("FILTER_TOPIC", "export-worker-test-filters")


class Worker:
    """
    Basic example worker that outputs exports to the console.
    """
    def __init__(self, kafka_client: ew_lib.KafkaClient, filter_handler: ew_lib.filter.FilterHandler):
        self.__kafka_client = kafka_client
        self.__filter_handler = filter_handler
        self.__stop = False

    def stop(self):
        """
        Call to break while loop in run() method below.
        :return: None
        """
        self.__stop = True

    def run(self):
        """
        Will get and print exports indefinitely.
        :return: None
        """
        while not self.__stop:
            exports = self.__kafka_client.get_exports(timeout=1.0)
            if exports:
                print(exports)


# Initialize a KafkaFilterConsumer to consume filters from a Kafka topic.
kafka_filter_consumer = ew_lib.filter.KafkaFilterConsumer(
    metadata_broker_list=METADATA_BROKER_LIST,
    group_id=FILTER_CONSUMER_GROUP_ID,
    filter_topic=FILTER_TOPIC
)

# Initialize a FilterHandler with the above KafkaFilterConsumer.
filter_handler = ew_lib.filter.FilterHandler(filter_consumer=kafka_filter_consumer)

# Initialize a Kafka consumer to consume messages to be filtered.
kafka_message_consumer = confluent_kafka.Consumer(
    {
        "metadata.broker.list": METADATA_BROKER_LIST,
        "group.id": KAFKA_CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky"
    }
)

# Initialize a KakfaClient by providing a FilterHandler and Kafka Consumer.
kafka_client = ew_lib.KafkaClient(
    kafka_consumer=kafka_message_consumer,
    filter_handler=filter_handler
)

# Initialize the example Worker by providing a KafkaClient and FilterHandler.
worker = Worker(kafka_client=kafka_client, filter_handler=filter_handler)


def handle_shutdown(signo, stack_frame):
    """
    Ensure a clean shutdown by stopping active threads and closing consumers.
    """
    print(f"got signal '{signo}': exiting ...")
    worker.stop()
    kafka_client.stop()
    filter_handler.stop()
    kafka_filter_consumer.close()
    kafka_message_consumer.close()


# Register relevant signals to be handled by the above function.
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# Start the FilterHandler, KfakaClient and example Worker.
filter_handler.start()
kafka_client.start()
worker.run()
