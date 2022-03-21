import ew_lib
import confluent_kafka
import signal
import os
import threading

# Environment variables for configuration. See 'docker-compose.yml' for more information.
METADATA_BROKER_LIST = os.getenv("METADATA_BROKER_LIST")
DATA_CONSUMER_GROUP_ID = os.getenv("DATA_CONSUMER_GROUP_ID")
FILTER_CONSUMER_GROUP_ID = os.getenv("FILTER_CONSUMER_GROUP_ID")
FILTER_TOPIC = os.getenv("FILTER_TOPIC")


class Worker:
    """
    Basic example worker that outputs exports to the console.
    """
    def __init__(self, data_client: ew_lib.DataClient):
        self.__data_client = data_client
        self.__event = threading.Event()
        self.__stop = False
        self.__err = False

    def set_event(self, err):
        """
        Callback method for filter synchronisation event.
        :param err: Bool indicating if synchronisation was successful.
        :return: None
        """
        self.__err = err
        self.__event.set()

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
        self.__event.wait()
        if not self.__err:
            while not self.__stop:
                exports = self.__data_client.get_exports(timeout=1.0)
                if exports:
                    print(exports)


# Initialize a FilterClient to consume filters from a kafka topic.
filter_client = ew_lib.FilterClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": METADATA_BROKER_LIST,
            "group.id": FILTER_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    ),
    filter_topic=FILTER_TOPIC
)

# Initialize a DataClient by providing a kafka consumer and FilterClient.
data_client = ew_lib.DataClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": METADATA_BROKER_LIST,
            "group.id": DATA_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky"
        }
    ),
    filter_client=filter_client
)

# Initialize the example Worker by providing a DataClient and event object.
worker = Worker(data_client=data_client)

# Set the event.set method as a callback.
filter_client.set_on_sync(worker.set_event)


def handle_shutdown(signo, stack_frame):
    """
    Ensure a clean shutdown by stopping active threads.
    """
    print(f"got '{signal.Signals(signo).name}': exiting ...")
    worker.stop()
    data_client.stop()
    filter_client.stop()


# Register relevant signals to be handled by the above function.
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# Start the FilterClient, DataClient and example Worker.
filter_client.start()
data_client.start()
worker.run()
