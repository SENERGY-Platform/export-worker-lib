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


class TestDataClient(unittest.TestCase):
    def _init(self, msg_errors=False, handle_offsets=False):
        filter_client = init_filter_client(filters=filters)
        mock_kafka_consumer = MockKafkaConsumer(data=data, sources=True, msg_error=msg_errors)
        data_client = ew_lib.DataClient(
            kafka_consumer=mock_kafka_consumer,
            filter_client=filter_client,
            handle_offsets=handle_offsets,
            kafka_msg_err_ignore=[3],
            logger=test_logger
        )
        return data_client

    def test_get_exports(self):
        data_client = self._init()

    def test_get_exports_batch(self):
        data_client = self._init()

    def test_store_offsets(self):
        data_client = self._init(handle_offsets=True)

    def test_kafka_message_errors(self):
        data_client = self._init(msg_errors=True)
