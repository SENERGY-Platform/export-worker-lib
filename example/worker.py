import ew_lib
import confluent_kafka
import signal
import os
import logging

# Environment variables for configuration. See 'docker-compose.yml' for more information.
METADATA_BROKER_LIST = os.getenv("METADATA_BROKER_LIST")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID")
FILTER_CONSUMER_GROUP_ID = os.getenv("FILTER_CONSUMER_GROUP_ID")
FILTER_TOPIC = os.getenv("FILTER_TOPIC")

# Set ew_lib logger to debug
ew_lib_logger = logging.getLogger('ew-lib')
ew_lib_logger.setLevel(logging.DEBUG)


class Worker:
    """
    Basic example worker that outputs exports to the console.
    """
    def __init__(self, kafka_data_client: ew_lib.clients.KafkaDataClient):
        self.__kafka_data_client = kafka_data_client
        self.__stop = False

    def stop(self):
        """
        Call to break loop of the 'run' method.
        :return: None
        """
        self.__stop = True

    def run(self):
        """
        Will get and print exports indefinitely.
        :return: None
        """
        while not self.__stop:
            exports = self.__kafka_data_client.get_exports(timeout=1.0)
            if exports:
                print(exports)


# Initialize a FilterHandler.
filter_handler = ew_lib.filter.FilterHandler()

# Initialize a KafkaFilterClient to consume filters from a kafka topic.
kafka_filter_client = ew_lib.clients.KafkaFilterClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": METADATA_BROKER_LIST,
            "group.id": FILTER_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    ),
    filter_handler=filter_handler,
    filter_topic=FILTER_TOPIC
)

# Initialize a KafkaDataClient by providing a kafka consumer and FilterHandler.
kafka_data_client = ew_lib.clients.KafkaDataClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": METADATA_BROKER_LIST,
            "group.id": KAFKA_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky"
        }
    ),
    filter_handler=filter_handler
)

# Initialize the example Worker by providing a KafkaDataClient.
worker = Worker(kafka_data_client=kafka_data_client)


def handle_shutdown(signo, stack_frame):
    """
    Ensure a clean shutdown by stopping active threads.
    """
    print(f"got signal '{signo}': exiting ...")
    worker.stop()
    kafka_data_client.stop()
    kafka_filter_client.stop()


# Register relevant signals to be handled by the above function.
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# Start the KafkaFilterClient, KafkaDataClient and example Worker.
kafka_filter_client.start()
kafka_data_client.start()
worker.run()
