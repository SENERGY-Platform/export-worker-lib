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

from .. import exceptions, builders
from .._util import logger, handle_kafka_error, log_kafka_sub_action, validate
from ..filter import FilterHandler
import uuid
import typing
import confluent_kafka
import threading
import json
import time


class KafkaDataClient:
    __log_msg_prefix = "kafka data client"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self, kafka_consumer: confluent_kafka.Consumer, filter_handler: FilterHandler, builder=builders.dict_builder, subscribe_interval: int = 5):
        validate(kafka_consumer, confluent_kafka.Consumer, "kafka_consumer")
        validate(filter_handler, FilterHandler, "filter_handler")
        self.__consumer = kafka_consumer
        self.__filter_handler = filter_handler
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
                timestamp = self.__filter_handler.get_sources_timestamp()
                if self.__sources_timestamp != timestamp:
                    sources = self.__filter_handler.get_sources()
                    with self.__lock:
                        if sources:
                            self.__consumer.subscribe(
                                sources,
                                on_assign=KafkaDataClient.__on_assign,
                                on_revoke=KafkaDataClient.__on_revoke,
                                on_lost=KafkaDataClient.__on_lost
                            )
                    self.__sources_timestamp = timestamp
                time.sleep(self.__subscribe_interval)
            except Exception as ex:
                logger.error(f"{KafkaDataClient.__log_err_msg_prefix}: handling subscriptions failed: {ex}")
        self.__consumer.close()

    @staticmethod
    def __on_assign(_, p):
        log_kafka_sub_action("assign", p, KafkaDataClient.__log_msg_prefix)

    @staticmethod
    def __on_revoke(_, p):
        log_kafka_sub_action("revoke", p, KafkaDataClient.__log_msg_prefix)

    @staticmethod
    def __on_lost(_, p):
        log_kafka_sub_action("lost", p, KafkaDataClient.__log_msg_prefix)

    def get_exports(self, timeout: float) -> typing.Optional[typing.Dict[str, typing.Any]]:
        with self.__lock:
            msg_obj = self.__consumer.poll(timeout=timeout)
            if msg_obj:
                if not msg_obj.error():
                    try:
                        filtered_data = self.__filter_handler.process_message(
                            message=json.loads(msg_obj.value()),
                            source=msg_obj.topic(),
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
                        logger.error(f"{KafkaDataClient.__log_err_msg_prefix}: {ex}")
                else:
                    handle_kafka_error(
                        msg_obj=msg_obj,
                        text=KafkaDataClient.__log_err_msg_prefix
                    )

    def get_exports_batch(self, timeout: float, limit: int) -> typing.Optional[typing.Dict[str, typing.List[typing.Any]]]:
        with self.__lock:
            msg_obj_list = self.__consumer.consume(num_messages=limit, timeout=timeout)
            if msg_obj_list:
                exports_batch = dict()
                for msg_obj in msg_obj_list:
                    if not msg_obj.error():
                        try:
                            filtered_data = self.__filter_handler.process_message(
                                message=json.loads(msg_obj.value()),
                                source=msg_obj.topic(),
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
                            logger.error(f"{KafkaDataClient.__log_err_msg_prefix}: {ex}")
                    else:
                        handle_kafka_error(
                            msg_obj=msg_obj,
                            text=KafkaDataClient.__log_err_msg_prefix,
                            raise_error=False
                        )
                if exports_batch:
                    return exports_batch

    def start(self):
        self.__thread.start()

    def stop(self):
        self.__stop = True
        self.__thread.join()
