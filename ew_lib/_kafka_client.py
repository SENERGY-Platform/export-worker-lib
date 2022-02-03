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

__all__ = ("KafkaClient", )

from . import exceptions, builders
from ._util import logger, handle_kafka_error
from .filter import FilterConsumer
from .filter._handler import FilterHandler
import uuid
import typing
import confluent_kafka
import threading
import json
import time


class KafkaClient:
    __log_msg_prefix = "kafka client"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, kafka_consumer: confluent_kafka.Consumer, filter_consumer: FilterConsumer, builder=builders.dict_builder, subscribe_interval: int = 5):
        if not isinstance(kafka_consumer, confluent_kafka.Consumer):
            raise TypeError(f"{type(kafka_consumer)} !=> {confluent_kafka.Consumer}")
        self.__consumer = kafka_consumer
        self.__filter_handler = FilterHandler(filter_consumer=filter_consumer)
        self.__builder = builder
        self.__subscribe_interval = subscribe_interval
        self.__thread = threading.Thread(
            name=f"{self.__class__.__name__}-{uuid.uuid4()}",
            target=self.__handle_subscriptions,
            daemon=True
        )
        self.__lock = threading.Lock()
        self.__sources_timestamp = None
        self.__stop = False

    def __handle_subscriptions(self):
        while not self.__stop:
            try:
                timestamp = self.__filter_handler.sources_timestamp
                if self.__sources_timestamp != timestamp:
                    sources = self.__filter_handler.sources
                    with self.__lock:
                        if sources:
                            self.__consumer.subscribe(
                                sources,
                                on_assign=KafkaClient.__on_assign,
                                on_revoke=KafkaClient.__on_revoke,
                                on_lost=KafkaClient.__on_lost
                            )
                    self.__sources_timestamp = timestamp
                time.sleep(self.__subscribe_interval)
            except Exception as ex:
                logger.error(f"{KafkaClient.__log_err_msg_prefix}: handling subscriptions failed: {ex}")

    @staticmethod
    def __log_sub_action(action: str, partitions: typing.List[confluent_kafka.TopicPartition]):
        for partition in partitions:
            logger.info(
                f"{KafkaClient.__log_msg_prefix}: subscription event: action={action} topic={partition.topic} partition={partition.partition} offset={partition.offset}"
            )

    @staticmethod
    def __on_assign(_, p):
        KafkaClient.__log_sub_action("assign", p)

    @staticmethod
    def __on_revoke(_, p):
        KafkaClient.__log_sub_action("revoke", p)

    @staticmethod
    def __on_lost(_, p):
        KafkaClient.__log_sub_action("lost", p)

    def get_exports(self, timeout: float) -> typing.Optional[typing.Dict[str, typing.Any]]:
        with self.__lock:
            msg_obj = self.__consumer.poll(timeout=timeout)
            if msg_obj:
                if not msg_obj.error():
                    try:
                        filtered_data = self.__filter_handler.filter_message(
                            msg=json.loads(msg_obj.value()),
                            builder=self.__builder
                        )
                        exports = dict()
                        for data in filtered_data:
                            for export_id in data[1]:
                                exports[export_id] = data[0]
                        return exports
                    except (exceptions.MessageIdentificationError, exceptions.NoFilterError):
                        pass
                    except exceptions.FilterMessageError as ex:
                        logger.error(f"{KafkaClient.__log_err_msg_prefix}: {ex}")
                else:
                    handle_kafka_error(
                        msg_obj=msg_obj,
                        text=KafkaClient.__log_err_msg_prefix
                    )

    def get_exports_batch(self, timeout: float, limit: int) -> typing.Optional[typing.Dict[str, typing.List[typing.Any]]]:
        with self.__lock:
            msg_obj_list = self.__consumer.consume(num_messages=limit, timeout=timeout)
            if msg_obj_list:
                exports_batch = dict()
                for msg_obj in msg_obj_list:
                    if not msg_obj.error():
                        try:
                            filtered_data = self.__filter_handler.filter_message(
                                msg=json.loads(msg_obj.value()),
                                builder=self.__builder
                            )
                            for data in filtered_data:
                                for export_id in data[1]:
                                    if export_id not in exports_batch:
                                        exports_batch[export_id] = [data[0]]
                                    else:
                                        exports_batch[export_id].append(data[0])
                        except (exceptions.MessageIdentificationError, exceptions.NoFilterError):
                            pass
                        except exceptions.FilterMessageError as ex:
                            logger.error(f"{KafkaClient.__log_err_msg_prefix}: {ex}")
                    else:
                        handle_kafka_error(
                            msg_obj=msg_obj,
                            text=KafkaClient.__log_err_msg_prefix,
                            raise_error=False
                        )
                if exports_batch:
                    return exports_batch

    def exports(self, timeout: float):
        while True:
            exports = self.get_exports(timeout=timeout)
            if exports:
                yield exports

    def exports_batch(self, timeout: float, limit: int):
        while True:
            exports_batch = self.get_exports_batch(timeout=timeout, limit=limit)
            if exports_batch:
                yield exports_batch

    def begin(self):
        self.__filter_handler.start()
        self.__thread.start()

    def close(self):
        self.__stop = True
        self.__filter_handler.stop()
        self.__consumer.close()
        self.__filter_handler.join()
        self.__thread.join()
