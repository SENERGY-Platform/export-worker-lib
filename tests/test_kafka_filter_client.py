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
import unittest

from ._util import *
from .test_filter_handler import TestFilterHandlerBase
import ew_lib
import time
import threading


class TestKafkaFilterClient(unittest.TestCase, TestFilterHandlerBase):
    def _init_filter_handler(self, filters, **kwargs):
        test_kafka_consumer = TestKafkaConsumer(data=filters, sources=False)
        filter_handler = ew_lib.filter.FilterHandler()
        kafka_filter_client = ew_lib.clients.KafkaFilterClient(
            kafka_consumer=test_kafka_consumer,
            filter_handler=filter_handler,
            filter_topic="filters"
        )
        kafka_filter_client.start()
        while not test_kafka_consumer.empty():
            time.sleep(0.1)
        kafka_filter_client.stop()
        return filter_handler


class TestKafkaFilterClientSyncCallback(unittest.TestCase, TestFilterHandlerBase):
    def _callback(self):
        self._event.set()

    def _init_filter_handler(self, filters, timeout=False):
        self._event = threading.Event()
        test_kafka_consumer = TestKafkaConsumer(data=filters, sources=False)
        filter_handler = ew_lib.filter.FilterHandler()
        kafka_filter_client = ew_lib.clients.KafkaFilterClient(
            kafka_consumer=test_kafka_consumer,
            filter_handler=filter_handler,
            filter_topic="filters",
            on_sync=self._callback,
            sync_delay=5
        )
        self.assertFalse(self._event.is_set())
        kafka_filter_client.start()
        if timeout:
            self._event.wait(timeout=15)
            self.assertFalse(self._event.is_set())
        else:
            self._event.wait()
            self.assertTrue(self._event.is_set())
        kafka_filter_client.stop()
        return filter_handler
