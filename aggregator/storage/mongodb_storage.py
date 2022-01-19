"""MongoDB storage interface"""
import datetime
import pandas
from pymongo import MongoClient
import ssl
import os
import logging
from bson.json_util import dumps
from bson.objectid import ObjectId
from pandas.io.json import json_normalize
import json
from aggregator.datacleaner import DataCleaner
from anomaly_detector.storage.storage_attribute import MGStorageAttribute
from anomaly_detector.storage.mongodb_storage import MongoDBStorage


_LOGGER = logging.getLogger(__name__)


class MongoDBDataStorageSource(DataCleaner, MongoDBStorage):
    """MongoDB data source implementation."""

    NAME = "mg.source"

    def __init__(self, config):
        """Initialize mongodb storage backend."""
        self.config = config
        MongoDBStorage.__init__(self, config)

    def retrieve(self, storage_attribute: MGStorageAttribute):
        """Retrieve data from MongoDB."""

        mg_input_db = self.mg[self.config.MG_INPUT_DB]
        now = datetime.datetime.now()

        mg_data = mg_input_db[self.config.MG_INPUT_COL]

        if self.config.LOGSOURCE_HOSTNAME != 'localhost':
            query = {
                self.config.DATETIME_INDEX:  {
                    '$gte': now - datetime.timedelta(seconds=storage_attribute.time_range),
                    '$lt': now
                },
                self.config.HOSTNAME_INDEX: self.config.LOGSOURCE_HOSTNAME
            }
        else:
            query = {
                self.config.DATETIME_INDEX:  {
                    '$gte': now - datetime.timedelta(seconds=storage_attribute.time_range),
                    '$lt': now
                }
            }

        mg_data = mg_data.find(query).sort(self.config.DATETIME_INDEX, -1).limit(storage_attribute.number_of_entries)
        _LOGGER.info(
            "Reading %d log entries in last %d seconds from %s",
            mg_data.count(True),
            storage_attribute.time_range,
            self.config.MG_HOST,
        )

        self.mg.close()

        if not mg_data.count():   # if it equials 0:
            return pandas.DataFrame(), mg_data

        mg_data = dumps(mg_data, sort_keys=False)
        mg_data_normalized = pandas.DataFrame(pandas.json_normalize(json.loads(mg_data)))
        _LOGGER.info("%d logs loaded in from last %d seconds", len(mg_data_normalized),
                     storage_attribute.time_range)
        self._preprocess(mg_data_normalized)
        return mg_data_normalized, json.loads(mg_data)


class MongoDBDataSink(DataCleaner, MongoDBStorage):
    """MongoDB data sink implementation."""

    NAME = "mg.sink"

    def __init__(self, config):
        """Initialize mongodb storage backend."""
        self.config = config
        MongoDBStorage.__init__(self, config)

    def store_results(self, data, original_messages):
        """Store results back to MongoDB"""
        mg_input_db = self.mg[self.config.MG_INPUT_DB]
        mg_input_col = mg_input_db[self.config.MG_INPUT_COL]
        mg_target_db = self.mg[self.config.MG_TARGET_DB]
        mg_target_col = mg_target_db[self.config.MG_TARGET_COL]
        _LOGGER.info("Inderting data to MongoDB.")
        for aggr_data in data:
            to_insert = aggr_data.copy()
            del to_insert["original_msgs_ids"]
            for _id in aggr_data["original_msgs_ids"]:
                mg_input_col.update_one(
                    {
                        "_id": _id
                     },
                    {
                        "$set": {
                            "aggregated_message_id": aggr_data["_id"]
                        }
                    }, upsert=False)
            mg_target_col.insert_one(to_insert)
