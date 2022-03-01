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

__all__ = ("KafkaFilterClient", )

from .. import exceptions
from .._util import logger, handle_kafka_error, model, log_kafka_sub_action, validate
from ..filter import FilterHandler
import typing
import uuid
import confluent_kafka
import threading
import json


class KafkaFilterClient:
    __log_msg_prefix = "kafka filter client"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, kafka_consumer: confluent_kafka.Consumer, filter_handler: FilterHandler, filter_topic: str, poll_timeout: float = 1.0):
        validate(kafka_consumer, confluent_kafka.Consumer, "kafka_consumer")
        validate(filter_handler, FilterHandler, "filter_handler")
        validate(filter_topic, str, "filter_topic")
        self.__consumer = kafka_consumer
        self.__filter_handler = filter_handler
        self.__thread = threading.Thread(
            name=f"{self.__class__.__name__}-{uuid.uuid4()}",
            target=self.__consume_filters,
            daemon=True
        )
        self.__consumer.subscribe(
            [filter_topic],
            on_assign=self.__on_assign,
            on_revoke=KafkaFilterClient.__on_revoke,
            on_lost=KafkaFilterClient.__on_lost
        )
        self.__poll_timeout = poll_timeout
        self.__reset = True
        self.__stop = False

    def __on_assign(self, consumer: confluent_kafka.Consumer, partitions: typing.List[confluent_kafka.TopicPartition]):
        if self.__reset:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
            consumer.assign(partitions)
            self.__reset = False
        log_kafka_sub_action("assign", partitions, KafkaFilterClient.__log_msg_prefix)

    def __consume_filters(self) -> None:
        while not self.__stop:
            try:
                msg_obj = self.__consumer.poll(timeout=self.__poll_timeout)
                if msg_obj:
                    if not msg_obj.error():
                        try:
                            msg_obj = json.loads(msg_obj.value())
                            method = msg_obj[model.FilterMessage.method]
                            if method == model.Methods.put:
                                self.__filter_handler.add_filter(msg_obj[model.FilterMessage.payload])
                            elif method == model.Methods.delete:
                                self.__filter_handler.delete_filter(**msg_obj[model.FilterMessage.payload])
                            else:
                                raise exceptions.MethodError(method)
                            logger.debug(
                                f"{KafkaFilterClient.__log_msg_prefix}: method={method} payload={msg_obj[model.FilterMessage.payload]}"
                            )
                        except Exception as ex:
                            logger.error(f"{KafkaFilterClient.__log_err_msg_prefix}: handling message failed: {ex}")
                    else:
                        handle_kafka_error(
                            msg_obj=msg_obj,
                            text=KafkaFilterClient.__log_err_msg_prefix
                        )
            except Exception as ex:
                logger.error(f"{KafkaFilterClient.__log_err_msg_prefix}: consuming message failed: {ex}")
        self.__consumer.close()

    @staticmethod
    def __on_revoke(_, p):
        log_kafka_sub_action("revoke", p, KafkaFilterClient.__log_msg_prefix)

    @staticmethod
    def __on_lost(_, p):
        log_kafka_sub_action("lost", p, KafkaFilterClient.__log_msg_prefix)

    def start(self):
        self.__thread.start()

    def stop(self):
        self.__stop = True
        self.__thread.join()
