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

---

### Filters

Filters are used to identify messages and extract data to be provided as exports.
A filter is composed of an export ID, a source from which the messages originate, a mapping for data extraction as well as type conversion, and optional message identifiers.

The JSON data structure of a filter is shown below:

```JSON
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

#### Mapping

A mapping is specified as a JSON object. A key consists of a target path under which data is stored in the export and a target type to which the data is to be converted. 
The source path to the message data to be extracted is specified as the value:

```JSON
{
  "<target path>:<target type>": "<source path>"
}
```

#### Identifiers

Identifiers allow messages to be identified by their content and structure. 
The use of identifiers makes it possible to differentiate messages and apply appropriate mappings.
This is relevant when messages with different structures originate from the same or multiple sources and an allocation via the source is not possible. 
Or messages with the same structure are to be distinguished by their content.
Identifiers are specified as a list of JSON objects. An identifier must have a "key" field and optionally a "value" field:

```JSON
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
