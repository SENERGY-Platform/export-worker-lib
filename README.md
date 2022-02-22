export-worker-lib
================

Library for implementing export-workers to transfer data from the SENERGY streaming platform to data sinks.

----------

+ [Installation](#installation)
  + [Install](#install)
  + [Update](#update)
  + [Uninstall](#uninstall)
+ [Quickstart](#quickstart)

----------

### Installation

#### Install

`pip install git+https://github.com/SENERGY-Platform/export-worker-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

#### Upgrade

`pip install --upgrade git+https://github.com/SENERGY-Platform/export-worker-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

#### Uninstall

`pip uninstall export-worker-lib`

### Quickstart

The current version only offers integration with Kafka but users can create their own clients to consume data and filters.

    # Initialize a KafkaFilterConsumer to consume filters from a Kafka topic.
    kafka_filter_consumer = ew_lib.filter.KafkaFilterConsumer(
        metadata_broker_list="<your kafka broker>",
        group_id="filter-consumer",
        filter_topic="filters"
    )
    
    # Initialize a FilterHandler.
    filter_handler = ew_lib.filter.FilterHandler(filter_consumer=kafka_filter_consumer)
    
    # Initialize a Kafka consumer to consume messages to be filtered.
    kafka_message_consumer = confluent_kafka.Consumer(
        {
            "metadata.broker.list": "<your kafka broker>",
            "group.id": "data-consumer",
            "auto.offset.reset": "earliest"
        }
    )
    
    # Initialize a KakfaClient.
    kafka_client = ew_lib.KafkaClient(
        kafka_consumer=kafka_message_consumer,
        filter_handler=filter_handler
    )

    # Get exports.
    while True:
        exports = kafka_client.get_exports(timeout=1.0)
        if exports:
            ...

For more details please refer to the [example](https://github.com/SENERGY-Platform/export-worker-lib/tree/master/example) contained within this repository.
