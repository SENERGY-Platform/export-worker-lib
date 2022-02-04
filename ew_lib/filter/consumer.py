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

__all__ = ("FilterConsumer", "KafkaFilterConsumer")

from .. import exceptions
from .._util import logger, handle_kafka_error
import typing
import abc
import confluent_kafka
import json


class FilterConsumer(abc.ABC):
    @abc.abstractmethod
    def get_filter(self) -> typing.Dict:
        pass


class KafkaFilterConsumer(FilterConsumer):
    __log_msg_prefix = "kafka filter consumer"

    def __init__(self, metadata_broker_list: str, group_id: str, filter_topic: str, poll_timeout: float = 1.0):
        self.__consumer = confluent_kafka.Consumer(
            {
                "metadata.broker.list": metadata_broker_list,
                "group.id": group_id,
                "auto.offset.reset": "earliest"
            },
            logger=logger
        )
        self.__consumer.subscribe([filter_topic], on_assign=self.__on_assign)
        self.__poll_timeout = poll_timeout
        self.__reset = True

    def __on_assign(self, consumer: confluent_kafka.Consumer, partitions: typing.List[confluent_kafka.TopicPartition]):
        if self.__reset:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
            consumer.assign(partitions)
            self.__reset = False

    def get_filter(self) -> typing.Dict:
        msg_obj = self.__consumer.poll(timeout=self.__poll_timeout)
        if msg_obj:
            if not msg_obj.error():
                return json.loads(msg_obj.value())
            else:
                handle_kafka_error(
                    msg_obj=msg_obj,
                    text=f"{KafkaFilterConsumer.__log_msg_prefix} error"
                )

    def close(self):
        self.__consumer.close()
