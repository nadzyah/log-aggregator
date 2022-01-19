"""MySQL storage interface"""
import datetime
import pandas
import mysql.connector
import logging
import json
from anomaly_detector.storage.storage import DataCleaner
from anomaly_detector.storage.storage_source import StorageSource
from anomaly_detector.storage.stdout_sink import StorageSink
from anomaly_detector.storage.storage_attribute import MySQLStorageAttribute

_LOGGER = logging.getLogger(__name__)

class MySQLStorage:
    """MySQL storage backend."""

    NAME = "mysql"
    _MESSAGE_FIELD_NAME = "_source.message"

    def __init__(self, config, is_input=True):
        """Initialize MySQL storage backend."""
        self.config = config
        self._connect(is_input)

    def _connect(self, is_input):
        if is_input:
            _LOGGER.warning(
                "Connection to MySQL at %s" % (self.config.MYSQL_INPUT_HOST)
            )
            self.db = mysql.connector.connect(
                host=self.config.MYSQL_INPUT_HOST,
                port=self.config.MYSQL_INPUT_PORT,
                user=self.config.MYSQL_INPUT_USER,
                password=self.config.MYSQL_INPUT_PASSWORD,
                database=self.config.MYSQL_INPUT_DB
            )
            return
        _LOGGER.warning(
                "Connection to MySQL at %s" % (self.config.MYSQL_TARGET_HOST)
            )
        self.db = mysql.connector.connect(
            host=self.config.MYSQL_TARGET_HOST,
            port=self.config.MYSQL_TARGET_PORT,
            user=self.config.MYSQL_TARGET_USER,
            password=self.config.MYSQL_TARGET_PASSWORD,
            database=self.config.MYSQL_TARGET_DB
        )

class MySQLDataStorageSource(StorageSource, DataCleaner, MySQLStorage):
    """MySQL data source implementation."""

    NAME = "mysql.source"

    def __init__(self, config):
        """Initialize MySQL storage backend."""
        self.config = config
        MySQLStorage.__init__(self, config)

    def retrieve(self, storage_attribute: MySQLStorageAttribute):
        """Retrieve data from MySQL"""
        now = datetime.datetime.now()

        cursor = self.db.cursor()

        if self.config.LOGSOURCE_HOSTNAME != 'localhost':
            sql = "SELECT logid, %s, %s, %s, anomaly_score FROM %s WHERE (%s BETWEEN '%s' AND '%s' AND %s = '%s') ORDER BY %s DESC LIMIT %d" % (
                self.config.MESSAGE_INDEX,
                self.config.DATETIME_INDEX,
                self.config.HOSTNAME_INDEX,
                self.config.MYSQL_INPUT_TABLE,
                self.config.DATETIME_INDEX,
                (now - datetime.timedelta(seconds=storage_attribute.time_range)).strftime("%Y-%m-%d %H:%M:%S"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                self.config.HOSTNAME_INDEX,
                self.config.LOGSOURCE_HOSTNAME,
                self.config.DATETIME_INDEX,
                storage_attribute.number_of_entries
            )
        else:
            sql = "SELECT logid, %s, %s, %s, anomaly_score FROM %s WHERE (%s BETWEEN '%s' AND '%s') ORDER BY %s DESC LIMIT %d" % (
                self.config.MESSAGE_INDEX,
                self.config.DATETIME_INDEX,
                self.config.HOSTNAME_INDEX,
                self.config.MYSQL_INPUT_TABLE,
                self.config.DATETIME_INDEX,
                (now - datetime.timedelta(seconds=storage_attribute.time_range)).strftime("%Y-%m-%d %H:%M:%S"),
                now.strftime("%Y-%m-%d %H:%M:%S"),
                self.config.DATETIME_INDEX,
                storage_attribute.number_of_entries
            )

        cursor.execute(sql)
        data = cursor.fetchall()
        json_data = []
        for x in data:
            tmp = {}
            tmp["logid"] = x[0]
            tmp[self.config.MESSAGE_INDEX] = x[1]
            tmp[self.config.DATETIME_INDEX] = x[2]
            tmp[self.config.HOSTNAME_INDEX] = x[3]
            tmp["anomaly_score"] = x[4]
            json_data.append(tmp)

        _LOGGER.info(
            "Reading %d log entries in last %d seconds from %s",
            len(json_data),
            storage_attribute.time_range,
            self.config.MYSQL_INPUT_HOST,
        )

        if not len(json_data):
            return pandas.DataFrame(), json_data

        json_data_normalized = pandas.DataFrame(pandas.json_normalize(json_data))

        _LOGGER.info("%d logs loaded in from last %d seconds", len(json_data_normalized),
                     storage_attribute.time_range)

        self._preprocess(json_data_normalized)

        cursor.close()

        return json_data_normalized, json_data

class MySQLDataSink(StorageSink, DataCleaner, MySQLStorage):
    """MySQL data sink implementation."""

    NAME = "mysql.sink"

    def __init__(self, config):
        """Initialize MySQL storage backend."""
        self.config = config
        self.input_db = mysql.connector.connect(
            host=self.config.MYSQL_INPUT_HOST,
            port=self.config.MYSQL_INPUT_PORT,
            user=self.config.MYSQL_INPUT_USER,
            password=self.config.MYSQL_INPUT_PASSWORD,
            database=self.config.MYSQL_INPUT_DB
        )
        self.target_db = mysql.connector.connect(
            host=self.config.MYSQL_TARGET_HOST,
            port=self.config.MYSQL_TARGET_PORT,
            user=self.config.MYSQL_TARGET_USER,
            password=self.config.MYSQL_TARGET_PASSWORD,
            database=self.config.MYSQL_TARGET_DB
        )

    def store_results(self, data, original_messages):
        """Store results bach to MySQL"""
        input_cursor = self.input_db.cursor(buffered=True)
        target_cursor = self.target_db.cursor(buffered=True)
        _LOGGER.info("Inderting data to MySQL.")
        for aggr_data in data:
            to_insert = aggr_data.copy()
            del to_insert["original_msgs_ids"]
            to_insert["message"] = "'" + to_insert["message"] + "'"
            to_insert["hostname"] = "'" + to_insert["hostname"] + "'"

            insert_sql= "INSERT INTO " + self.config.MYSQL_TARGET_TABLE
            insert_sql += " (" + ", ".join(map(str, list(to_insert.keys()))) + ") VALUES ("
            insert_sql += ", ".join(map(str, list(to_insert.values()))) + ")"
            target_cursor.execute(insert_sql)
            self.target_db.commit()

            update_sql = "UPDATE %s SET aggr_msg_id = %s WHERE logid in (%s)" % (
                self.config.MYSQL_INPUT_TABLE,
                aggr_data["aggr_msg_id"],
                ", ".join(map(str, aggr_data["original_msgs_ids"]))
            )
            input_cursor.execute(update_sql)
            self.input_db.commit()

        input_cursor.close()
        target_cursor.close()
