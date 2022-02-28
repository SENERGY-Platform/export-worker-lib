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
      "metadata.broker.list": "<your kafka broker>",
      "group.id": "data-consumer",
      "auto.offset.reset": "earliest",
      "partition.assignment.strategy": "cooperative-sticky"
    }
  ),
  filter_handler=filter_handler
)

# Get exports.
while not stop:
  exports = kafka_data_client.get_exports(timeout=1.0)
  if exports:
    ...

# Stop clients when done.
kafka_data_client.stop()
kafka_filter_client.stop()
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
A filter is composed of an export ID, a source from which the messages originate, a mapping for data extraction as well as type conversion, and optional message identifiers.

The structure of a filter is shown below:

```python
{
  "export_id": "<export id>",
  "source": "<message source>",
  "mapping": {
    "<target path>:<target type>": "<source path>"
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
}
```

### Mapping

A mapping is specified as dictionary. A key consists of a target path under which data is stored in the export and a target type to which the data is to be converted. 
The source path to the message data to be extracted is specified as the value:

```python
{
  "<target path>:<target type>": "<source path>"
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

Create a FilterHandler object via: `ew_lib.filter.FilterHandler()`

FilterHandler objects provide the following methods:

`add_filter(filter)`
+ Add a filter with the structure defined in [Filters](#filters). The _filter_ argument requires a dictionary.
Raises HashMappingError, AddMessageIdentifierError, AddExportError, AddMappingError, AddSourceError and AddFilterError.

`delete_filter(export_id)`
+ Removes a filter by passing the ID of an export as a string to the _export_id_ argument.
Raises DeleteExportError, DeleteMessageIdentifierError, DeleteMappingError, DeleteSourceError and DeleteFilterError.

`get_sources()`
+ Returns a list of strings containing all sources added by filters.

`get_sources_timestamp()`
+ Returns a timestamp as a string that indicates the last time a filter was added or removed.

`get_export_metadata(export_id)`
+ Returns a dictionary with the source and the identifiers of a filter that corresponds to the export ID provided as a string to the _export_id_ argument.
Raises NoFilterError.

`process_message(message, source, builder)`
+ This method is used to apply filters by passing a message as a dictionary to the _message_ argument. 
Optionally, the source of the message can be passed as a string to the _source_ argument and a custom [builder](#builders) to the _builder_ argument.
The method returns a list of tuples, which in turn contain the extracted data and the corresponding export IDs.
Raises FilterMessageError, NoFilterError, MessageIdentificationError and MappingError.

## Clients

Clients consume data and / or filters, pass them to a FilterHandler object and provide the resulting exports, for further processing or storage.
Clients can be regarded as optional wrappers for FilterHandler objects and are created by the user.

In order to integrate with the SENERGY streaming platform, a client for consuming filters and a client for consuming data from Kafka are included in this repository:

### KafkaFilterClient

The KafkaFilterClient class consumes filters from a kafka topic and passes them to a FilterHandler object.
Filters are kept in memory, therefore filter consumption starts at the beginning of the topic.
This is handled by a background thread that consumes and processes messages that use the following JSON structure:

```json
{
  "method": "",
  "payload": {}
}
```

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
      "<target path>:<target type>": "<source path>"
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
  }
}
```

If the **delete** method is used, only an export ID must be specified:

```json
{
  "method": "delete",
  "payload": {
    "export_id": "<export id>"
  }
}
```

#### API

Create a KafkaFilterClient object by providing a confluent kafka [Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer) object, a FilterHandler object and the topic from which filters are to be consumed:
`ew_lib.clients.KafkaFilterClient(self, kafka_consumer, filter_handler, filter_topic, poll_timeout=1.0)`
If used in a cluster the group ID of the kafka consumer must be unique.

KafkaFilterClient objects provide the following methods:

`start()`
+ Starts the background thread.

`stop()`
+ Stops the background thread. Blocks until the execution of the thread has finished.

### KafkaDataClient

## Builders

### Dictionary builder

### String list builder

### Tuple list builder