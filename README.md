export-worker-lib
================

Library for implementing export-workers to transfer data from the SENERGY streaming platform to data sinks.

---

+ [Quickstart](#quickstart)
+ [Installation](#installation)
+ [FilterClient](#filterclient)
+ [DataClient](#dataclient)

---

## Quickstart

```python
import ew_lib
import confluent_kafka

# Initialize a FilterClient to consume filters from a kafka topic.
filter_client = ew_lib.FilterClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": "<your kafka broker>",
            "group.id": "filter-consumer",
            "auto.offset.reset": "earliest",
        }
    ),
    filter_topic="filters"
)

# Initialize a DataClient by providing a kafka consumer and FilterClient.
data_client = ew_lib.DataClient(
    kafka_consumer=confluent_kafka.Consumer(
        {
            "metadata.broker.list": "<your kafka broker>",
            "group.id": "data-consumer",
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky"
        }
    ),
    filter_client=filter_client
)

# Get exports.
while not stop:
    exports = data_client.get_exports(timeout=1.0)
    if exports:
        ...

# Stop clients when done.
data_client.stop()
filter_client.stop()
```

For more details please refer to the [example](https://github.com/SENERGY-Platform/export-worker-lib/tree/master/example) contained within this repository.

## Installation

### Install

`pip install git+https://github.com/SENERGY-Platform/export-worker-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

### Upgrade

`pip install --upgrade git+https://github.com/SENERGY-Platform/export-worker-lib.git@X.X.X`

Replace 'X.X.X' with the desired version.

### Uninstall

`pip uninstall export-worker-lib`

## FilterClient

The KafkaFilterClient class consumes filters from a kafka topic and passes them to a FilterHandler object.
Filters are kept in memory by the FilterHandler object, therefore filter consumption starts at the beginning of the topic to ensure that all filters ar present after every startup.
This is handled by a background thread that consumes and processes messages that use the following JSON structure:

```json
{
  "method": null,
  "payload": null,
  "timestamp": null
}
```

The _method_ is specified as a string, the _payload_ as a dictionary, and the _timestamp_ as either an integer, float, or string.

### Methods

~~To add or remove filters, the methods _put_ and _delete_ are available. 
The payload of the message varies depending on the method used.~~

~~If the **put** method is used, the payload corresponds to the structure defined in [Filters](#filters):~~

~~If the **delete** method is used, only an export ID must be specified:~~

### API

~~Create a FilterClient object by providing a confluent kafka [Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer) object, a FilterHandler object and the topic from which filters are to be consumed:~~

~~KafkaFilterClient objects provide the following methods:~~

`set_on_sync(callable, sync_delay=30)`: ~~Set a callback for when filters have been synchronised. 
The _callable_ argument requires a function and the _sync_delay_ argument defines how long the client will wait for new messages if the previously consumed messages are too old to determine a synchronised state.~~

`start()`: ~~Starts the background thread.~~

`stop()`: ~~Stops the background thread. Blocks until the execution of the thread has finished.~~

## DataClient

~~The KafkaDataClient class consumes messages from any number of kafka topics and passes them to a FilterHandler object to get exports, and provides them to the user.
For this purpose, a background thread automatically subscribes to topics that are specified as sources in filters stored by a FilterHandler object.~~

### API

~~Create a KafkaDataClient object by providing a confluent kafka [Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer) object, a FilterHandler object and an optional [builder](#builders) function:~~

~~KafkaDataClient objects provide the following methods:~~

`get_exports(timeout)`: ~~Consumes one message and passes it to a FilterHandler object for processing.
Returns a dictionary containing exports for the consumed message `{"<export id>": <data object>, ...}` or `None` if no matching filters are present or if the _timeout_ period as been exceeded and no message has been consumed.~~

`get_exports_batch(timeout, limit)`: ~~Consumes many messages and passes them to a FilterHandler object for processing. 
The _limit_ argument defines the maximum number of messages that can be consumed.
Returns a dictionary containing exports for the consumed messages `{"<export id>": [<data object>, ...], ...}` or `None` if no matching filters are present or if the _timeout_ period as been exceeded and no messages have been consumed.~~

`start()`: ~~Starts the background thread.~~

`stop()`: ~~Stops the background thread. Blocks until the execution of the thread has finished.~~