import datetime
import logging
import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

from anomaly_detector.storage.storage_attribute import MGStorageAttribute

from aggregator.storage.mongodb_storage import MongoDBDataStorageSource, MongoDBDataSink
from aggregator.datacleaner import DataCleaner
from aggregator.models.word2vec import W2VModel

_LOGGER = logging.getLogger(__name__)

class Aggregator:

    def __init__(self, config):
        self.config = config
        self.source_storage_catalog = {'mg': self._get_logs_from_mg}
        self.sink_storage_catalog = {'mg': self._write_logs_to_mg}

    def _get_logs_from_mg(self):
        """Retrieve data from MongoDB
        """
        mg = MongoDBDataStorageSource(self.config)
        mg_attr = MGStorageAttribute(self.config.AGGR_TIME_SPAN,
                                     self.config.AGGR_MAX_ENTRIES)
        return mg.retrieve(mg_attr)

    def _write_logs_to_mg(self, data):
        """Write data to MongoDB

        :param data: data in json format which should be pushed to DB
        """
        mg = MongoDBDataSink(self.config)
        mg.store_results(data)

    def _get_mean_time(self, time_list):
        """Return mean time in ISO format

        :param timelist: the list of timestamps in absolute format
        """
        mean = np.mean(time_list)
        return datetime.datetime.fromtimestamp(mean / 1e3)

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
        for cluster in np.unique(clusters):
            messages = []
            for i in list(df.loc[df['cluster'] == cluster].index):
                messages.append(logs_json[i]["message"])

            if cluster == -1:
                for msg in messages:
                    aggregated.append((msg, 1, []))
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
                aggregated.append((result_string[:-1], msg_num, messages))
                _LOGGER.info("%s logs were aggregated into: %s", msg_num, result_string[:-1])
        return aggregated

    def aggregated_logs_to_json(self, aggregated_logs, orig_df):
        """Format each message to list of dict with mean timestamp

        :param aggregated_logs: list of tuples (triplets) that represent message,
                                total number of original messages in the cluster
                                and the list of original messages
        :param orig_df: logs dataframe with the original messages

        Return list of dicts with the log messages, which should be pushes to database
        """
        dates = np.array(orig_df[self.config.DATETIME_INDEX + ".$date"]).astype(np.int64)
        mean_time = self._get_mean_time(dates)

        result = []
        for msg, total_num, messages in aggregated_logs:
            data = {}
            data["message"] = msg
            data["total_logs"] = total_num
            data["timestamp"] = mean_time
            data["was_added_at"] = datetime.datetime.now()
            if messages:
                data["original_messages"] = messages
            result.append(data)

        return result

    def aggregator(self):
        """The main function for the aggregator"""
        logs_df, logs_json = self.source_storage_catalog[self.config.STORAGE_DATASOURCE]()
        if logs_df.empty:
            _LOGGER.info("No logs were detected")
            return
        logs_list = list(logs_df["message"])
        w2v = W2VModel(self.config)
        vectors = w2v.get_vectors(logs_list)
        logs_as_vectors = w2v.vectorized_logs_to_single_vectors(vectors)
        clusters = self.get_clusters(logs_as_vectors)

        df = pd.DataFrame(list(zip(logs_list, clusters)),
                          columns =['message', 'cluster'])
        aggr_logs = self.aggregate_logs(df, logs_json, clusters)
        aggr_json = self.aggregated_logs_to_json(aggr_logs, logs_df)
        self.sink_storage_catalog[self.config.STORAGE_DATASINK](aggr_json)
        return aggr_json

