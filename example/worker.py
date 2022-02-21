import ew_lib
import confluent_kafka
import signal
import os

METADATA_BROKER_LIST = os.getenv("METADATA_BROKER_LIST")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "export-worker-test")
FILTER_CONSUMER_GROUP_ID = os.getenv("FILTER_CONSUMER_GROUP_ID")
FILTER_TOPIC = os.getenv("FILTER_TOPIC", "export-worker-test-filters")


class Worker:
    def __init__(self, kafka_client: ew_lib.KafkaClient, filter_handler: ew_lib.filter.FilterHandler):
        self.__kafka_client = kafka_client
        self.__filter_handler = filter_handler
        self.__stop = False

    def stop(self):
        self.__stop = True

    def run(self):
        while not self.__stop:
            exports = self.__kafka_client.get_exports(timeout=1.0)
            if exports:
                print(exports)


kafka_filter_consumer = ew_lib.filter.KafkaFilterConsumer(
    metadata_broker_list=METADATA_BROKER_LIST,
    group_id=FILTER_CONSUMER_GROUP_ID,
    filter_topic=FILTER_TOPIC
)
filter_handler = ew_lib.filter.FilterHandler(filter_consumer=kafka_filter_consumer)

kafka_message_consumer = confluent_kafka.Consumer(
    {
        "metadata.broker.list": METADATA_BROKER_LIST,
        "group.id": KAFKA_CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky"
    }
)
kafka_client = ew_lib.KafkaClient(
    kafka_consumer=kafka_message_consumer,
    filter_handler=filter_handler
)

worker = Worker(kafka_client=kafka_client, filter_handler=filter_handler)


def handle_sigterm(signo, stack_frame):
    print(f"got signal '{signo}': exiting ...")
    worker.stop()
    kafka_client.stop()
    filter_handler.stop()
    kafka_filter_consumer.close()
    kafka_message_consumer.close()


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

filter_handler.start()
kafka_client.start()
worker.run()
