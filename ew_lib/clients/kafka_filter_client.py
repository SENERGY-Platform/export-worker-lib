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
from .._util import logger, handle_kafka_error, log_kafka_sub_action, validate
from ..filter import FilterHandler
import typing
import uuid
import confluent_kafka
import threading
import json
import datetime


class Methods:
    put = "put"
    delete = "delete"


class Message:
    method = "method"
    payload = "payload"
    timestamp = "timestamp"


class KafkaFilterClient:
    """
    Consumes messages which contain instructions to create or delete filters.
    """
    __log_msg_prefix = "kafka filter client"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, kafka_consumer: confluent_kafka.Consumer, filter_handler: FilterHandler, filter_topic: str, poll_timeout: float = 1.0, time_format: typing.Optional[str] = None, utc: bool = True):
        """
        Creates a KafkaFilterClient object.
        :param kafka_consumer: A confluent_kafka.Consumer object.
        :param filter_handler: A ew_lib.filter.FilterHandler object.
        :param filter_topic: Kafka topic from which filters are to be consumed.
        :param poll_timeout: Maximum time in seconds to block waiting for message.
        :param time_format: Timestamp format (https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes). Only required if timestamps are provided as strings.
        :param utc: Set if timestamps are in UTC. Default is true.
        """
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
        self.__time_format = time_format
        self.__utc = utc
        self.__on_sync_callable = None
        self.__sync_delay = None
        self.__reset = True
        self.__stop = False
        self.__sync = False

    def __get_time(self):
        return datetime.datetime.utcnow().timestamp() if self.__utc else datetime.datetime.now().timestamp()

    def __handle_sync(self, time_a, time_b):
        if time_a >= time_b:
            self.__sync = True
            logger.debug(f"{KafkaFilterClient.__log_msg_prefix}: filters synchronized")
            try:
                self.__on_sync_callable()
            except Exception as ex:
                logger.error(f"{KafkaFilterClient.__log_err_msg_prefix}: sync callback failed: {ex}")

    def __consume_filters(self) -> None:
        start_time = None
        last_item_time = 0
        while not self.__stop:
            try:
                msg_obj = self.__consumer.poll(timeout=self.__poll_timeout)
                if msg_obj:
                    if not msg_obj.error():
                        try:
                            msg_obj = json.loads(msg_obj.value())
                            method = msg_obj[Message.method]
                            if self.__time_format:
                                timestamp = datetime.datetime.strptime(
                                    msg_obj[Message.timestamp],
                                    self.__time_format
                                ).timestamp()
                            else:
                                timestamp = msg_obj[Message.timestamp]
                                validate(timestamp, (float, int), "timestamp")
                            if method == Methods.put:
                                self.__filter_handler.add_filter(msg_obj[Message.payload])
                            elif method == Methods.delete:
                                self.__filter_handler.delete_filter(**msg_obj[Message.payload])
                            else:
                                raise exceptions.MethodError(method)
                            if self.__on_sync_callable and not self.__sync:
                                if not start_time:
                                    start_time = self.__get_time()
                                last_item_time = self.__get_time()
                                self.__handle_sync(timestamp, start_time)
                            logger.debug(
                                f"{KafkaFilterClient.__log_msg_prefix}: method={method} timestamp={timestamp} payload={msg_obj[Message.payload]}"
                            )
                        except Exception as ex:
                            logger.error(f"{KafkaFilterClient.__log_err_msg_prefix}: handling message failed: {ex}")
                    else:
                        handle_kafka_error(
                            msg_obj=msg_obj,
                            text=KafkaFilterClient.__log_err_msg_prefix
                        )
                else:
                    if self.__on_sync_callable and not self.__sync:
                        if start_time:
                            self.__handle_sync(self.__get_time() - last_item_time, self.__sync_delay)
            except Exception as ex:
                start_time = None
                last_item_time = 0
                logger.error(f"{KafkaFilterClient.__log_err_msg_prefix}: consuming message failed: {ex}")
        self.__consumer.close()

    def __on_assign(self, consumer: confluent_kafka.Consumer, partitions: typing.List[confluent_kafka.TopicPartition]):
        if self.__reset:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING
            consumer.assign(partitions)
            self.__reset = False
        log_kafka_sub_action("assign", partitions, KafkaFilterClient.__log_msg_prefix)

    @staticmethod
    def __on_revoke(_, p):
        log_kafka_sub_action("revoke", p, KafkaFilterClient.__log_msg_prefix)

    @staticmethod
    def __on_lost(_, p):
        log_kafka_sub_action("lost", p, KafkaFilterClient.__log_msg_prefix)

    def set_on_sync(self, callable: typing.Optional[typing.Callable], sync_delay: int = 30):
        """
        Set a callback for when filters have been synchronised.
        :param callable: Function to be executed. Must not block.
        :param sync_delay: Defines how long in seconds the client will wait for new messages if the previously consumed messages are too old to determine a synchronised state.
        :return: None
        """
        if self.__thread.is_alive():
            raise exceptions.SetCallbackError(callable)
        self.__on_sync_callable = callable
        self.__sync_delay = sync_delay

    def start(self):
        """
        Starts the background thread.
        :return: None
        """
        self.__thread.start()

    def stop(self):
        """
        Stops and joins the background thread.
        :return: None
        """
        self.__stop = True
        self.__thread.join()
