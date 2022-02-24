export-worker-lib
================

Library for implementing export-workers to transfer data from the SENERGY streaming platform to data sinks.

----------

+ [Installation](#installation)
  + [Install](#install)
  + [Upgrade](#Upgrade)
  + [Uninstall](#uninstall)
+ [Quickstart](#quickstart)
+ [Filters](#filters)

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

---

### Quickstart

The current version only offers integration with Kafka but users can create their own clients to consume data and filters.

    import ew_lib
    import confluent_kafka
    
    # Initialize a FilterHandler.
    filter_handler = ew_lib.filter.FilterHandler()

    # Initialize a KafkaFilterClient to consume filters.
    kafka_filter_client = ew_lib.clients.KafkaFilterClient(
        kafka_consumer=confluent_kafka.Consumer(
            {
                "metadata.broker.list": "<your kafka broker>",
                "group.id": "filter-consumer",
                "auto.offset.reset": "earliest",
            }
        ),
        filter_handler=filter_handler,
        filter_topic="filters"
    )
    
    # Initialize a KafkaDataClient to consume data and get exports.
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

    # Get exports.
    while True:
        exports = kafka_data_client.get_exports(timeout=1.0)
        if exports:
            ...

For more details please refer to the [example](https://github.com/SENERGY-Platform/export-worker-lib/tree/master/example) contained within this repository.

---

### Filters

Filters are consumed by the FilterHandler and determine which data from which sources are provided as exports.