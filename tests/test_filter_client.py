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

from ._util import *
import unittest
import ew_lib
import time


class TestFilterClient(unittest.TestCase):
    def _init(self, filters, msg_errors=False, sync_event=None):
        mock_kafka_consumer = MockKafkaConsumer(data=filters, sources=False, msg_error=msg_errors)
        filter_client = ew_lib.FilterClient(
            kafka_consumer=mock_kafka_consumer,
            filter_topic="filter",
            logger=test_logger
        )
        if sync_event:
            filter_client.set_on_sync(sync_event.set, 5)
        filter_client.start()
        if sync_event:
            sync_event.wait(timeout=10)
        else:
            while not mock_kafka_consumer.empty():
                time.sleep(0.1)
        filter_client.stop()
        filter_client.join()
        return filter_client

    def test_get_last_update(self):
        filter_client = self._init(filters=filters)
        self.assertIsNotNone(filter_client.get_last_update())

    def test_set_on_sync(self):
        event = SyncEvent()
        self._init(filters=filters, sync_event=event)
        self.assertTrue(event.is_set())
        self.assertFalse(event.err)

    def test_kafka_message_error(self):
        event = SyncEvent()
        self._init(filters=filters, sync_event=event, msg_errors=True)
        self.assertTrue(event.is_set())
        self.assertTrue(event.err)
