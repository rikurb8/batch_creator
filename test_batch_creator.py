import unittest
import random
import string
from batch_creator import BatchCreator


class TestBatchCreator(unittest.TestCase):

    @staticmethod
    def generate_records(count=10000, minimum_record_size=1, max_record_size=1000):
        mock_records = []

        for _ in range(count):
            mock_records.append(''.join(random.choices(string.ascii_letters,
                                                       k=random.randint(minimum_record_size, max_record_size))))

        return mock_records

    def test_batchify_max_records_in_batch(self):
        config = {
            'max_record_size_in_mb': 1,
            'max_batch_size_in_mb': 5,
            'max_records_in_batch': 500
        }
        batch_creator = BatchCreator(config=config)

        result_batches = batch_creator.batchify(records=self.generate_records())

        for batch in result_batches:
            self.assertLessEqual(len(batch), config['max_records_in_batch'], "Should have allowed number of records")

    def test_batchify_max_record_size_in_mb(self):
        config = {
            'max_record_size_in_mb': 1,
            'max_batch_size_in_mb': 5,
            'max_records_in_batch': 500
        }
        batch_creator = BatchCreator(config=config)

        too_large_record = ''.join(random.choices(string.ascii_letters, k=2000000))
        ok_record = "asd"
        result_batches = batch_creator.batchify(records=[too_large_record, ok_record])

        self.assertEqual(len(result_batches), 1, "Should contain only 1 batch")
        self.assertEqual(len(result_batches[0]), 1, "Should contain only 1 record in batch")
        self.assertEqual(result_batches[0][0], ok_record, "Should contain ok record")

    def test_batchify_max_batch_size_in_mb(self):
        config = {
            'max_record_size_in_mb': 1,
            'max_batch_size_in_mb': 5,
            'max_records_in_batch': 500
        }
        batch_creator = BatchCreator(config=config)

        result_batches = batch_creator.batchify(records=self.generate_records(count=100,
                                                                              minimum_record_size=500000,
                                                                              max_record_size=750000))
        max_allowed_batch_size_bytes = config['max_batch_size_in_mb'] * 1000000

        for batch in result_batches:
            total_size = 0
            for record in batch:
                total_size += len(record.encode('utf-8'))

            self.assertLessEqual(total_size, max_allowed_batch_size_bytes, "Should not contain batches larger than max size")


if __name__ == '__main__':
    unittest.main()
