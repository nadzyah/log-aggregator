import datetime
import logging
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from pprint import pprint
from bson.objectid import ObjectId

from anomaly_detector.storage.storage_attribute import MGStorageAttribute, MySQLStorageAttribute
from aggregator.storage.mongodb_storage import MongoDBDataStorageSource, MongoDBDataSink
from aggregator.storage.mysql_storage import MySQLDataStorageSource, MySQLDataSink, MySQLStorage
from aggregator.datacleaner import DataCleaner
from aggregator.models.word2vec import W2VModel

_LOGGER = logging.getLogger(__name__)

class Aggregator:

    def __init__(self, config):
        self.config = config
        self.source_storage_catalog = {'mg': self._get_logs_from_mg,
                                       'mysql': self._get_logs_from_mysql,
                                       }
        self.sink_storage_catalog = {'mg': self._write_logs_to_mg,
                                     "stdout": pprint,
                                     'mysql': self._write_logs_to_mysql,
                                     }

    def _get_logs_from_mg(self):
        """Retrieve data from MongoDB"""
        mg = MongoDBDataStorageSource(self.config)
        mg_attr = MGStorageAttribute(self.config.AGGR_TIME_SPAN,
                                     self.config.AGGR_MAX_ENTRIES)
        return mg.retrieve(mg_attr)

    def _write_logs_to_mg(self, data, original_messages):
        """Write data to MongoDB

        :param data: data in json format which should be pushed to DB
        """
        mg = MongoDataSink(self.config)
        mg.store_results(data, original_messages)

    def _get_logs_from_mysql(self):
        """Retrieve data from MySQL"""
        mysql = MySQLDataStorageSource(self.config)
        mysql_attr = MySQLStorageAttribute(self.config.AGGR_TIME_SPAN,
                                           self.config.AGGR_MAX_ENTRIES)
        return mysql.retrieve(mysql_attr)

    def _write_logs_to_mysql(self, data, original_messages):
        """Write data to MongoDB

        :param data: data in json format which should be pushed to DB
        """
        mg = MySQLDataSink(self.config)
        mg.store_results(data, original_messages)


    def _get_last_aggr_msg_id(self):
        mysql = MySQLStorage(self.config, is_input=False)
        sql = 'SELECT MAX(aggr_msg_id) FROM %s' % self.config.MYSQL_TARGET_TABLE
        cursor = mysql.db.cursor()
        data = cursor.execute(sql)
        if data:
            return data[0]
        return 0


    def _get_mean_time(self, time_list):
        """Return mean time in ISO format

        :param timelist: the list of timestamps in absolute format
        """
        if not isinstance(time_list, list):
            return time_list
        if not isinstance(time_list[0], int):
            tmp = []
            for x in time_list:
                tmp.append(x.timestamp())
            mean = float(np.mean(tmp))
            return datetime.datetime.fromtimestamp(mean)
        mean = int(np.mean(time_list))
        return datetime.datetime.fromtimestamp(mean / 1e3) - datetime.timedelta(hours=3)


    def get_clusters(self, vectors):
        """Clusterize logs and return clusters array

        :params vectors: list of vectors, which represent log messages
        """
        dbscan = DBSCAN(eps=self.config.AGGR_EPS,
                        min_samples=self.config.AGGR_MIN_SAMPLES)
        clusters = dbscan.fit_predict(vectors)
        _LOGGER.info("%s clusters were detected with DBSCAN algorithm", np.unique(clusters))
        return clusters

    def aggregate_logs(self, df, logs_json, clusters):
        """Return list of aggregated messages with aggregated parameters

        :param df: dataframe of logs with "message" and "cluster" column
        :param logs_json: list of logs in json format, where all logs are python dicts with "message" key
        :param clusters: list of integers which correspond cluster label of each logs message


        The number of rows in df, dicts in logs_json and integers in clusters must be the same

        Result example:

        <190>date=2021-12-01 * ** *** logid="0100026003" type="event" subtype="system" level="information" vd="root" **** tz="+0300" logdesc="DHCP statistics" ***** ****** ******* msg="DHCP statistics"
        ... and the list of original messages

        """
        aggregated = []
        logs_json_with_relation = logs_json.copy()
        mysql_id_incr = 1
        last_aggr_msg_id = self._get_last_aggr_msg_id()
        for cluster in np.unique(clusters):
            logs = []
            messages = []
            timestamps = []
            hostnames = []
            anomaly_scores = []
            original_msgs_ids = []
            for i in list(df.loc[df['cluster'] == cluster].index):
                if self.config.STORAGE_DATASINK == 'mg':
                    logs.append({"anomaly_score": logs_json[i]["anomaly_score"],
                                 "hostname": logs_json[i][self.config.HOSTNAME_INDEX],
                                 "message": logs_json[i][self.config.MESSAGE_INDEX],
                                 "timestamp": self._get_mean_time(logs_json[i][self.config.DATETIME_INDEX]["$date"])
                                 })
                    timestamps.append([logs_json[i][self.config.DATETIME_INDEX]["$date"]])
                    original_msgs_ids.append(ObjectId(logs_json[i]["_id"]["$oid"]))
                elif self.config.STORAGE_DATASINK == 'mysql':
                    logs.append({"anomaly_score": logs_json[i]["anomaly_score"],
                                 "hostname": logs_json[i][self.config.HOSTNAME_INDEX],
                                 "message": logs_json[i][self.config.MESSAGE_INDEX],
                                 "timestamp": self._get_mean_time(logs_json[i][self.config.DATETIME_INDEX])
                                 })
                    timestamps.append(logs_json[i][self.config.DATETIME_INDEX])
                    original_msgs_ids.append(logs_json[i]["logid"])

                messages.append(logs_json[i]["message"])
                hostnames.append(logs_json[i][self.config.HOSTNAME_INDEX])
                anomaly_scores.append(logs_json[i]["anomaly_score"])


            if cluster == -1:
                for i in range(len(messages)):
                    if self.config.STORAGE_DATASINK == 'mg':
                        aggregated.append((ObjectId(),
                                           messages[i],
                                           1,
                                           self._get_mean_time(timestamps[i]),
                                           hostnames[i],
                                           anomaly_scores[i],
                                           [original_msgs_ids[i]],
                                           ))
                    elif self.config.STORAGE_DATASINK == 'mysql':
                        aggregated.append((last_aggr_msg_id + mysql_id_incr,
                                           messages[i],
                                           1,
                                           self._get_mean_time(timestamps[i]),
                                           hostnames[i],
                                           anomaly_scores[i],
                                           [original_msgs_ids[i]],
                                           ))
                        mysql_id_incr += 1
            else:
                splited_messages = [x.split() for x in messages]
                splited_transpose = [list(row) for row in zip(*splited_messages)]
                result_string = ""
                var_num = 0

                for x in splited_transpose:
                    if len(set(x)) == 1:
                        result_string += x[0] + " "
                    else:
                        result_string += "***" + " "

                msg_num = len(messages)

                cluster_df = df.loc[df['cluster'] == cluster]
                mean_time = self._get_mean_time(timestamps)
                anomaly_score = np.mean(anomaly_scores)
                # The most frequent hostname
                hostname = max(set(hostnames), key = hostnames.count)

                if self.config.STORAGE_DATASINK == 'mg':
                    aggregated.append((ObjectId(),
                                       result_string[:-1],
                                       msg_num,
                                       mean_time,
                                       hostname,
                                       anomaly_score,
                                       original_msgs_ids))

                elif self.config.STORAGE_DATASINK == 'mysql':
                    aggregated.append((last_aggr_msg_id + mysql_id_incr,
                                       result_string[:-1],
                                       msg_num,
                                       mean_time,
                                       hostname,
                                       anomaly_score,
                                       original_msgs_ids))
                    mysql_id_incr += 1

                _LOGGER.info("%s logs were aggregated into: %s", msg_num, result_string[:-1])
        return aggregated

    def aggregated_logs_to_json(self, aggregated_logs):
        """Format each message to list of dict with mean timestamp

        :param aggregated_logs: list of tuples (triplets) that represent message,
                                total number of original messages in the cluster
                                and the list of original messages
        :param orig_df: logs dataframe with the original messages

        Return list of dicts with the log messages, which should be pushes to database
        """

        result = []
        for (_id, msg, total_num, mean_time,
             hostname, anomaly_score, original_msgs_ids) in aggregated_logs:
            data = {}
            if self.config.STORAGE_DATASINK == 'mg':
                data["_id"] = _id
            if self.config.STORAGE_DATASINK == 'mysql':
                data["aggr_msg_id"] = _id
            data["message"] = msg
            data["total_logs"] = total_num
            data["average_datetime"] = "'" +  mean_time.strftime("%Y-%m-%d %H:%M:%S") + "'"
            data["hostname"] = hostname
            data["average_anomaly_score"] = anomaly_score
            data["was_added_at"] = "'" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "'"
            data["original_msgs_ids"] = original_msgs_ids
            result.append(data)
        return result

    def aggregator(self):
        """The main function for the aggregator"""
        logs_df, logs_json = self.source_storage_catalog[self.config.STORAGE_DATASOURCE]()
        if logs_df.empty:
            _LOGGER.info("No logs were detected")
            return
        logs_list = list(logs_df[self.config.MESSAGE_INDEX])
        w2v = W2VModel(self.config)
        vectors = w2v.get_vectors(logs_list)
        logs_as_vectors = w2v.vectorized_logs_to_single_vectors(vectors)
        clusters = self.get_clusters(logs_as_vectors)

        # Normalized logs with cluster lables as DF
        df = pd.DataFrame(list(zip(logs_list, clusters)),
                          columns =['message', 'cluster'])
        # Aggregate logs
        aggr_logs = self.aggregate_logs(df, logs_json, clusters)
        # Convert aggregated logs to json
        aggr_json = self.aggregated_logs_to_json(aggr_logs)
        self.sink_storage_catalog[self.config.STORAGE_DATASINK](aggr_json, logs_json)
        return aggr_json
