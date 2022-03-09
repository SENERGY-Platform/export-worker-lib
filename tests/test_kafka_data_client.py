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


class TestKafkaDataClient(unittest.TestCase):
    def __init_client(self, filters, data):
        test_kafka_consumer = TestKafkaConsumer(data=data)
        filter_handler = test_filter_ingestion(test_obj=self, filters=filters)
        kafka_data_client = ew_lib.clients.kafka.KafkaDataClient(
            kafka_consumer=test_kafka_consumer,
            filter_handler=filter_handler,
            subscribe_interval=1
        )
        kafka_data_client.start()
        return kafka_data_client, filter_handler, test_kafka_consumer

    def test_get_exports_good_filters(self):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters, data=data)
        count = 0
        while not test_kafka_consumer.empty():
            exports = kafka_data_client.get_exports(timeout=1.0)
            if exports:
                self.assertIn(str(exports), results)
                count += 1
        self.assertEqual(count, len(results) - 1)
        kafka_data_client.stop()

    def _test_get_exports_batch_good_filters(self, limit, results):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters, data=data)
        count = 0
        while not test_kafka_consumer.empty():
            exports_batch, _ = kafka_data_client.get_exports_batch(timeout=5.0, limit=limit)
            if exports_batch:
                self.assertIn(str(exports_batch), results)
                count += 1
        self.assertEqual(count, len(results) - 1)
        kafka_data_client.stop()

    def test_get_exports_batch_good_filters_l2(self):
        self._test_get_exports_batch_good_filters(limit=2, results=batch_results_l2)

    def test_get_exports_batch_good_filters_l3(self):
        self._test_get_exports_batch_good_filters(limit=3, results=batch_results_l3)

    def test_get_exports_erroneous_filters(self):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters_bad, data=data)
        while not test_kafka_consumer.empty():
            self.assertIsNone(kafka_data_client.get_exports(timeout=1.0))
        kafka_data_client.stop()

    def test_get_exports_batch_erroneous_filters(self):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters_bad, data=data)
        while not test_kafka_consumer.empty():
            exports_batch, _ = kafka_data_client.get_exports_batch(timeout=5.0, limit=2)
            self.assertEqual(len(exports_batch), 0)
        kafka_data_client.stop()

    def test_get_exports_bad_messages(self):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters, data=data_bad)
        while not test_kafka_consumer.empty():
            self.assertIsNone(kafka_data_client.get_exports(timeout=1.0))
        kafka_data_client.stop()

    def test_get_exports_batch_bad_messages(self):
        kafka_data_client, filter_handler, test_kafka_consumer = self.__init_client(filters=filters, data=data_bad)
        while not test_kafka_consumer.empty():
            exports_batch, _ = kafka_data_client.get_exports_batch(timeout=5.0, limit=2)
            self.assertEqual(len(exports_batch), 0)
        kafka_data_client.stop()
