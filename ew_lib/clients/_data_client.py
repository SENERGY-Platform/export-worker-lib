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

__all__ = ("KafkaDataClient", )

from ..exceptions import *
from .._util import *
import mf_lib
import uuid
import typing
import confluent_kafka
import threading
import json
import logging


class KafkaDataClient:
    """
    Consumes messages from any number of kafka topics and passes them to a FilterHandler object to get exports, and provides them to the user.
    """
    __logger = get_logger("ew-lib-kdc")
    __log_msg_prefix = "kafka data client"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, kafka_consumer: confluent_kafka.Consumer, filter_handler: mf_lib.FilterHandler, subscribe_interval: int = 5, handle_offsets: bool = False, kafka_msg_err_ignore: typing.Optional[typing.List] = None, logger: typing.Optional[logging.Logger] = None):
        """
        Creates a KafkaDataClient object.
        :param kafka_consumer: A confluent_kafka.Consumer object.
        :param filter_handler: A mf_lib.FilterHandler object.
        :param subscribe_interval: Specifies in seconds how often to check if new sources are available and subscriptions have to be made.
        :param handle_offsets: Set to true if enable.auto.offset.store is set to false.
        """
        validate(kafka_consumer, confluent_kafka.Consumer, "kafka_consumer")
        validate(filter_handler, mf_lib.filter.FilterHandler, "filter_handler")
        self.__consumer = kafka_consumer
        self.__filter_handler = filter_handler
        self.__subscribe_interval = subscribe_interval
        self.__offsets_handler = ConsumerOffsetHandler(kafka_consumer=kafka_consumer) if handle_offsets else None
        self.__kafka_error_ignore = kafka_msg_err_ignore or list()
        if logger:
            self.__logger = logger
        self.__thread = threading.Thread(
            name=f"{self.__class__.__name__}-{uuid.uuid4()}",
            target=self.__handle_subscriptions,
            daemon=True
        )
        self.__lock = threading.Lock()
        self.__sleeper = threading.Event()
        self.__sources_timestamp = None
        self.__stop = False

    def __handle_subscriptions(self):
        while not self.__stop:
            try:
                timestamp = self.__filter_handler.get_sources_timestamp()
                if self.__sources_timestamp != timestamp:
                    sources = self.__filter_handler.get_sources()
                    with self.__lock:
                        if sources:
                            self.__consumer.subscribe(
                                sources,
                                on_assign=self.__on_assign,
                                on_revoke=self.__on_revoke,
                                on_lost=self.__on_lost
                            )
                    self.__sources_timestamp = timestamp
                self.__sleeper.wait(self.__subscribe_interval)
            except Exception as ex:
                self.__logger.critical(f"{KafkaDataClient.__log_err_msg_prefix}: handling subscriptions failed: reason={get_exception_str(ex)}")
                self.__stop = True

    def __handle_msg_obj(self, msg_obj: confluent_kafka.Message, data_builder, extra_builder) -> typing.List[mf_lib.FilterResult]:
        exports = list()
        if self.__offsets_handler:
            self.__offsets_handler.add_offset(
                topic=msg_obj.topic(),
                partition=msg_obj.partition(),
                offset=msg_obj.offset()
            )
        try:
            for result in self.__filter_handler.get_results(message=json.loads(msg_obj.value()), source=msg_obj.topic(), data_builder=data_builder, extra_builder=extra_builder):
                if not result.ex:
                    exports.append(result)
                else:
                    log_message_error(
                        prefix=KafkaDataClient.__log_err_msg_prefix,
                        ex=result.ex,
                        message=msg_obj.value(),
                        logger=self.__logger
                    )
        except mf_lib.exceptions.NoFilterError:
            pass
        except mf_lib.exceptions.MessageIdentificationError as ex:
            log_message_error(
                prefix=KafkaDataClient.__log_err_msg_prefix,
                ex=ex,
                message=msg_obj.value(),
                logger=self.__logger
            )
        return exports

    def __on_assign(self, _, p):
        log_kafka_sub_action("assign", p, KafkaDataClient.__log_msg_prefix, self.__logger)

    def __on_revoke(self, _, p):
        log_kafka_sub_action("revoke", p, KafkaDataClient.__log_msg_prefix, self.__logger)

    def __on_lost(self, _, p):
        log_kafka_sub_action("lost", p, KafkaDataClient.__log_msg_prefix, self.__logger)

    def get_exports(self, timeout: float, data_builder=mf_lib.builders.dict_builder, extra_builder=mf_lib.builders.dict_builder) -> typing.Optional[typing.List[mf_lib.FilterResult]]:
        """
        Consumes one message and passes it to a FilterHandler object for processing.
        :param timeout: Maximum time in seconds to block waiting for message.
        :param extra_builder:
        :param data_builder:
        :return: List containing exports [(<data object>, <extra object>, ("<export id>", ...)), ...] or None.
        """
        with self.__lock:
            msg_obj = self.__consumer.poll(timeout=timeout)
            if msg_obj:
                if not msg_obj.error():
                    return self.__handle_msg_obj(msg_obj=msg_obj, data_builder=data_builder, extra_builder=extra_builder)
                else:
                    if msg_obj.error().code() not in self.__kafka_error_ignore:
                        raise KafkaMessageError(
                            msg=msg_obj.error().str(),
                            code=msg_obj.error().code(),
                            retry=msg_obj.error().retriable(),
                            fatal=msg_obj.error().fatal()
                        )

    def get_exports_batch(self, timeout: float, limit: int, data_builder=mf_lib.builders.dict_builder, extra_builder=mf_lib.builders.dict_builder) -> typing.Optional[typing.Tuple[typing.List[mf_lib.FilterResult], typing.List[KafkaMessageError]]]:
        """
        Consumes many messages and passes them to a FilterHandler object for processing.
        :param timeout: Maximum time in seconds to block waiting for messages.
        :param limit: Defines the maximum number of messages that can be consumed.
        :param extra_builder:
        :param data_builder:
        :return: None or a tuple with a list of exports [(<data object>, <extra object>, ("<export id>", ...)), ...] and a list of potential message exceptions.
        """
        with self.__lock:
            msg_obj_list = self.__consumer.consume(num_messages=limit, timeout=timeout)
            if msg_obj_list:
                exports_batch = list()
                msg_exceptions = list()
                for msg_obj in msg_obj_list:
                    if not msg_obj.error():
                        exports_batch += self.__handle_msg_obj(msg_obj=msg_obj, data_builder=data_builder, extra_builder=extra_builder)
                    else:
                        if msg_obj.error().code() not in self.__kafka_error_ignore:
                            ex = KafkaMessageError(
                                    msg=msg_obj.error().str(),
                                    code=msg_obj.error().code(),
                                    retry=msg_obj.error().retriable(),
                                    fatal=msg_obj.error().fatal()
                                )
                            msg_exceptions.append(ex)
                            self.__logger.error(f"{KafkaDataClient.__log_err_msg_prefix}: {ex}")
                return exports_batch, msg_exceptions

    def store_offsets(self):
        """
        Store offsets of last consumed messages.
        :return: None
        """
        self.__offsets_handler.store_offsets()

    def start(self):
        """
        Starts the background thread.
        :return: None
        """
        self.__thread.start()

    def stop(self):
        """
        Stops the background thread.
        :return: None
        """
        self.__stop = True
        self.__sleeper.set()

    def is_alive(self) -> bool:
        """
        Check if internal thread is alive.
        :return: True if alive and False if not.
        """
        return self.__thread.is_alive()

    def join(self):
        """
        Wait till the background thread is done.
        :return: None
        """
        self.__thread.join()
