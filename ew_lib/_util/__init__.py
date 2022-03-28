"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import logging
import confluent_kafka
import typing
import traceback


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.propagate = False
    return logger


def validate(obj, cls, name):
    assert obj, f"'{name}' can't be None"
    assert isinstance(obj, cls), f"'{name}' can't be of type '{type(obj).__name__}'"


def log_kafka_sub_action(action: str, partitions: typing.List, prefix: str, logger: logging.Logger):
    for partition in partitions:
        logger.info(
            f"{prefix}: subscription event: action={action} topic={partition.topic} partition={partition.partition} offset={partition.offset}"
        )


def log_message_error(prefix, ex, message, logger: logging.Logger):
    err_msg = f"{prefix}: {ex}"
    if logger.level == logging.DEBUG:
        err_msg += f" message={message}"
    logger.error(err_msg)


def get_exception_str(ex):
    return "[" + ", ".join([item.strip().replace("\n", " ") for item in traceback.format_exception_only(type(ex), ex)]) + "]"


class ConsumerOffsetHandler:
    def __init__(self, kafka_consumer: confluent_kafka.Consumer):
        self.__kafka_consumer = kafka_consumer
        self.__offsets = dict()

    def add_offset(self, topic: str, partition: int, offset: int):
        key = f"{topic}{partition}"
        offset += 1
        if key not in self.__offsets:
            self.__offsets[key] = confluent_kafka.TopicPartition(topic=topic, partition=partition, offset=offset)
        else:
            tp_obj = self.__offsets[key]
            tp_obj.offset = offset

    def store_offsets(self):
        if self.__offsets:
            self.__kafka_consumer.store_offsets(offsets=list(self.__offsets.values()))
            self.__offsets.clear()