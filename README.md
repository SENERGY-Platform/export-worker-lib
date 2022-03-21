export-worker-lib
================

Library for implementing export-workers to transfer data from the SENERGY streaming platform to data sinks.

---

+ [Quickstart](#quickstart)
+ [Installation](#installation)
+ [Filters](#filters)
+ [FilterHandler](#filterhandler)
+ [Clients](#clients)
+ [Builders](#builders)

---

## Quickstart

The current version only offers integration with Kafka but users can create their own clients to consume data and filters.

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

## Filters

Filters are used to identify messages and extract data to be provided as exports.
A filter is composed of an export ID, a source from which the messages originate, mappings for data extraction as well as type conversion, optional message identifiers and export arguments.
The latter can be any information that is necessary for the handling of an export.

The structure of a filter is shown below:

```python
{
    "source": "<message source>",
    "mappings": {
        "<target path>:<value type>:<mapping type>": "<source path>"
    },
    "identifiers": [
        {
            "key": "<message key name>",
            "value": "<message key value>"
        },
        {
            "key": "<message key name>"
        }
    ],
    "export_id": "<export id>",
    "export_args": {}
}
```

### Mappings

A mappings are specified as a dictionary. A key consists of a target path under which data is stored in the export, a value type to which the data is to be converted and a mapping type.
The mapping types _data_ and _extra_ are available. The _data_ type defines which data will be extracted for an export. 
Additional data that is relevant for handling an export, such as a timestamp, is extracted by the _extra_ type.
The source path to the message data to be extracted is specified as the value:

```python
{
    "<target path>:<value type>:<mapping type>": "<source path>"
}
```

### Identifiers

Identifiers allow messages to be identified by their content and structure. 
The use of identifiers makes it possible to differentiate messages and apply appropriate mappings.
This is relevant when messages with different structures originate from the same or multiple sources and an allocation via the source is not possible. 
Or messages with the same structure are to be distinguished by their content.
Identifiers are specified as a list of dictionaries. An identifier must have a "key" field and optionally a "value" field:

```python
[
    {
        "key": "<message key name>",
        "value": "<message key value>"
    },
    {
        "key": "<message key name>"
    }
]
```

The key field of an identifier specifies the name of a key that must be present in a message.
The Value field specifies a value for the key so that messages with the same data structures can be differentiated.
If no value field is used, the existence of the key referenced in the key field is sufficient for a message to be identified.

## FilterHandler

The FilterHandler class provides functionality for adding and removing filters as well as applying filters to messages and extracting data.

                 +---\        +---\                                                                    
                 |    ----+   |    ----+                                                               
                 | Filter |---| Filter |-\                                                             
                 |        |   |        |  \ +--------------+                              +----------+
                 +--------+   +--------+   >|              |   +---\        +---\         |          |
                                            |              |   |    ----+   |    ----+    |          |
                                            | FilterHandler|---| Export |---| Export |--->| Database |
                                            |              |   |        |   |        |    |          |
    +---\        +---\        +---\        >|              |   +--------+   +--------+    |          |
    |    ----+   |    ----+   |    ----+  / +--------------+                              +----------+
    | Message|---| Message|---| Message|-/                                                             
    |        |   |        |   |        |                                                               
    +--------+   +--------+   +--------+                                                               

### API

Create a FilterHandler object:

```python
ew_lib.filter.FilterHandler()
```

FilterHandler objects provide the following methods:

`add_filter(filter)`: Add a filter with the structure defined in [Filters](#filters). The _filter_ argument requires a dictionary.
Raises HashMappingError, AddMessageIdentifierError, AddExportError, AddMappingError, AddSourceError and AddFilterError.

`delete_filter(export_id)`: Removes a filter by passing the ID of an export as a string to the _export_id_ argument.
Raises DeleteExportError, DeleteMessageIdentifierError, DeleteMappingError, DeleteSourceError and DeleteFilterError.

`get_sources()`: Returns a list of strings containing all sources added by filters.

`get_sources_timestamp()`: Returns a timestamp that indicates the last time a filter was added or removed.

`get_export_metadata(export_id)`: Returns a dictionary with the source and the identifiers of a filter that corresponds to the export ID provided as a string to the _export_id_ argument.
Raises NoFilterError.

`process_message(message, source, builder)`: ~~This method is used to apply filters by passing a message as a dictionary to the _message_ argument. 
Optionally, the source of the message can be passed as a string to the _source_ argument and a custom [builder](#builders) to the _builder_ argument.
The method returns a list of tuples, which in turn contain the extracted data and the corresponding export IDs: `[(<data object>, ("<export id>", ...)), ...]`.
Raises FilterMessageError, NoFilterError, MessageIdentificationError and MappingError.~~

## Clients

Clients consume data and / or filters, pass them to a FilterHandler object and provide the resulting exports, for further processing or storage.
Clients can be regarded as optional wrappers for FilterHandler objects and are created by the user.

In order to integrate with the SENERGY streaming platform, a client for consuming filters and a client for consuming data from Kafka are included in this repository:

### KafkaFilterClient

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

#### Methods

To add or remove filters, the methods _put_ and _delete_ are available. 
The payload of the message varies depending on the method used.

If the **put** method is used, the payload corresponds to the structure defined in [Filters](#filters):

```json
{
  "method": "put",
  "payload": {
    "export_id": "<export id>",
    "source": "<message source>",
    "mapping": {
      "<target path>:<value type>:<mapping type>": "<source path>"
    },
    "identifiers": [
      {
        "key": "<message key name>",
        "value": "<message key value>"
      },
      {
        "key": "<message key name>"
      }
    ]
  },
  "timestamp": "2022-03-02T08:52:24Z"
}
```

If the **delete** method is used, only an export ID must be specified:

```json
{
  "method": "delete",
  "payload": {
    "export_id": "<export id>"
  },
  "timestamp": "2022-03-02T08:52:25Z"
}
```

#### API

Create a KafkaFilterClient object by providing a confluent kafka [Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer) object, a FilterHandler object and the topic from which filters are to be consumed:

```python
ew_lib.clients.KafkaFilterClient(kafka_consumer, filter_handler, filter_topic, poll_timeout=1.0, time_format=None, utc=True)
```

KafkaFilterClient objects provide the following methods:

`set_on_sync(callable, sync_delay=30)`: Set a callback for when filters have been synchronised. 
The _callable_ argument requires a function and the _sync_delay_ argument defines how long the client will wait for new messages if the previously consumed messages are too old to determine a synchronised state.

`start()`: Starts the background thread.

`stop()`: Stops the background thread. Blocks until the execution of the thread has finished.

### KafkaDataClient

The KafkaDataClient class consumes messages from any number of kafka topics and passes them to a FilterHandler object to get exports, and provides them to the user.
For this purpose, a background thread automatically subscribes to topics that are specified as sources in filters stored by a FilterHandler object.

#### API

Create a KafkaDataClient object by providing a confluent kafka [Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer) object, a FilterHandler object and an optional [builder](#builders) function:

```python
ew_lib.clients.KafkaDataClient(kafka_consumer, filter_handler, builder=builders.dict_builder, subscribe_interval=5)
```

KafkaDataClient objects provide the following methods:

`get_exports(timeout)`: ~~Consumes one message and passes it to a FilterHandler object for processing.
Returns a dictionary containing exports for the consumed message `{"<export id>": <data object>, ...}` or `None` if no matching filters are present or if the _timeout_ period as been exceeded and no message has been consumed.~~

`get_exports_batch(timeout, limit)`: ~~Consumes many messages and passes them to a FilterHandler object for processing. 
The _limit_ argument defines the maximum number of messages that can be consumed.
Returns a dictionary containing exports for the consumed messages `{"<export id>": [<data object>, ...], ...}` or `None` if no matching filters are present or if the _timeout_ period as been exceeded and no messages have been consumed.~~

`start()`: Starts the background thread.

`stop()`: Stops the background thread. Blocks until the execution of the thread has finished.

## Builders

Builder are functions that allow to customize the structure of export data according to the user's requirements.
Three builder functions are already provided by this repository:

### Dictionary builder

Stores data in a dictionary: `{"<key>": <value>, ...}`

### String list builder

Stores data as delimited key value strings in a list: `["<key>=<value>", ...]`

### Tuple list builder

Stores data as key value tuples in a list: `[(<key>, <value>), ...]`