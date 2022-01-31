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

from .._util import logger
import typing
import abc
import confluent_kafka
import json


class FilterConsumer(abc.ABC):
    @abc.abstractmethod
    def get_filter(self) -> typing.Dict:
        pass


class KafkaFilterConsumer(FilterConsumer):
    def __init__(self, brokers: typing.List, consumer_group: str, filter_topic: str, poll_timeout: float = 1.0):
        self.__kafka_consr_config = {
            "metadata.broker.list": ",".join(brokers),
            "group.id": consumer_group,
            "auto.offset.reset": "earliest"
        }
        self.__filter_topic = [filter_topic]
        self.__poll_timeout = poll_timeout
        self.__kafka_consumer: typing.Optional[confluent_kafka.Consumer] = None
        self.__reset = True

    def __on_assign(self, consumer: confluent_kafka.Consumer, partitions: typing.List[confluent_kafka.TopicPartition]):
        if self.__reset:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
            consumer.assign(partitions)
            self.__reset = False

    def start(self):
        self.__kafka_consumer = confluent_kafka.Consumer(self.__kafka_consr_config)
        self.__kafka_consumer.subscribe(self.__filter_topic, on_assign=self.__on_assign)

    def get_filter(self) -> typing.Dict:
        msg = self.__kafka_consumer.poll(timeout=self.__poll_timeout)
        if msg:
            if not msg.error():
                return json.loads(msg.value())
            else:
                logger.error(f"filter consumer message error: {msg.error()}")

    def close(self):
        self.__kafka_consumer.close()
