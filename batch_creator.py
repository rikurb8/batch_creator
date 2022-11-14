MEGABYTE = 1000000
DEFAULT_CONFIG = {
    'max_record_size_in_mb': 1,
    'max_batch_size_in_mb': 5,
    'max_records_in_batch': 500
}


class BatchCreator:
    """ Small utility that takes a list of Records (variable length strings) and splits them in to batches of records.
        Result is an array of arrays where batches follow the config below.

    Config:
        max_record_size_in_mb: Max allowed size of output record in MB. Larger records will be discarded. Default 1 MB
        max_batch_size_in_mb: Max size of output batch. Default 5 MB
        max_records_in_batch: Max amount of records in a batch. Default 500
    """

    def __init__(self, config=None):
        if config is None:
            self.__config = DEFAULT_CONFIG
        else:
            self.__config = config

        self.__all_batches = []
        self.__current_batch = []
        self.__current_batch_size = 0

    def batchify(self, records):
        """
        Splits the input in to batches using provided config, eg.

        ["record1", "record2"] -> [["record1", "record2]]


        :param records: List of variable length strings
        :return: Input list split in to batches.
        """

        for record in records:
            record_size = utf8_length_in_bytes(record)
            max_record_size = self.__config['max_record_size_in_mb'] * MEGABYTE
            max_batch_size = self.__config['max_batch_size_in_mb'] * MEGABYTE

            # Check that record size fits within limits
            if record_size > max_record_size:
                # TODO: store in DLQ if needed
                continue

            # Make sure batch isn't full
            if len(self.__current_batch) >= self.__config['max_records_in_batch']:
                self.__start_new_batch()

            # Make sure batch size limit is not exceeded after latest record
            if self.__current_batch_size + record_size > max_batch_size:
                self.__start_new_batch()

            self.__current_batch.append(record)
            self.__current_batch_size += record_size

        # Ensure latest batch is added to all_batches
        self.__start_new_batch()

        return self.__all_batches

    def __start_new_batch(self):
        if len(self.__current_batch) > 0:
            self.__all_batches.append(self.__current_batch)

        # Start collecting a new batch
        self.__current_batch = []
        self.__current_batch_size = 0


def utf8_length_in_bytes(s):
    return len(s.encode('utf-8'))
