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
    def _init(self, msg_errors=False, handle_offsets=False, sub_topics=None):
        filter_client = init_filter_client(filters=filters)
        mock_kafka_consumer = MockKafkaConsumer(data=data, sources=True, msg_error=msg_errors, sub_topics=sub_topics)
        data_client = ew_lib.DataClient(
            kafka_consumer=mock_kafka_consumer,
            filter_client=filter_client,
            handle_offsets=handle_offsets,
            kafka_msg_err_ignore=[3],
            logger=test_logger
        )
        return data_client, mock_kafka_consumer

    def test_get_exports(self):
        data_client, mock_kafka_consumer = self._init()
        count = 0
        while not mock_kafka_consumer.empty():
            exports = data_client.get_exports(timeout=1.0)
            self.assertIn(str(exports), export_results[count])
            count += 1

    def _test_get_exports_batch(self, limit, results):
        data_client, mock_kafka_consumer = self._init()
        count = 0
        while not mock_kafka_consumer.empty():
            exports_batch, _ = data_client.get_exports_batch(timeout=5.0, limit=limit)
            self.assertIn(str(exports_batch), results[count])
            count += 1

    def test_get_exports_batch_limit2(self):
        self._test_get_exports_batch(limit=2, results=batch_results_l2)

    def test_get_exports_batch_limit3(self):
        self._test_get_exports_batch(limit=3, results=batch_results_l3)

    def test_get_exports_kafka_message_errors(self):
        data_client, mock_kafka_consumer = self._init(msg_errors=True)
        count = 0
        while not mock_kafka_consumer.empty():
            try:
                exports = data_client.get_exports(timeout=1.0)
                if exports is not None:
                    self.assertIn(str(exports), export_results[count])
                    count += 1
            except Exception as ex:
                self.assertIsInstance(ex, ew_lib.exceptions.KafkaMessageError)

    def test_get_exports_batch_kafka_message_errors(self):
        data_client, mock_kafka_consumer = self._init(msg_errors=True)
        count = 0
        while not mock_kafka_consumer.empty():
            _, msg_exceptions = data_client.get_exports_batch(timeout=5.0, limit=2)
            for msg_ex in msg_exceptions:
                self.assertIsInstance(msg_ex, ew_lib.exceptions.KafkaMessageError)
                count += 1
        self.assertEqual(count, 4)

    def test_subscribe(self):
        data_client, mock_kafka_consumer = self._init(sub_topics=["src_1", "src_2"])
        data_client.start()
        while not mock_kafka_consumer.subscribed():
            time.sleep(0.1)
        data_client.stop()
        data_client.join()

    def test_store_offsets(self):
        data_client, mock_kafka_consumer = self._init(handle_offsets=True)
        while not mock_kafka_consumer.empty():
            data_client.get_exports(timeout=1.0)
            data_client.store_offsets()
