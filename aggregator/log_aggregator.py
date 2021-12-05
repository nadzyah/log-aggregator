import datetime

import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN

from anomaly_detector.storage.storage_attribute import MGStorageAttribute

from aggregator.storage.mongodb_storage import MongoDBDataStorageSource, MongoDBDataSink
from aggregator.datacleaner import DataCleaner
from aggregator.models.word2vec import W2VModel

class Aggregator:

    def __init__(self, config):
        self.config = config

    def _get_logs_from_mg(self):
        mg = MongoDBDataStorageSource(self.config)
        mg_attr = MGStorageAttribute(self.config.AGGR_TIME_SPAN,
                                     self.config.AGGR_MAX_ENTRIES)
        return mg.retrieve(mg_attr)

    def _write_logs_to_mg(self, data):
        mg = MongoDBDataSink(self.config)
        mg.store_results(data)

    def _get_mean_time(self, time_list):
        """Return mean time in ISO format
        """
        mean = np.mean(time_list)
        return datetime.datetime.fromtimestamp(mean / 1e3)

    def get_clusters(self, vectors):
        """Clusterize logs and return clusters array
        """
        dbscan = DBSCAN(eps=self.config.AGGR_EPS,
                        min_samples=self.config.AGGR_MIN_SAMPLES)
        clusters = dbscan.fit_predict(vectors)
        return clusters

    def aggregate_logs(self, df, logs_json, clusters):
        """Return list of aggregated messages with aggregated parameters
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

        return aggregated

    def aggregated_logs_to_json(self, aggregated_logs, orig_df):
        """Format each message to MongoDB dict with mean timestamp"""
        dates = np.array(orig_df[self.config.DATETIME_INDEX + ".$date"]).astype(np.int64)
        mean_time = self._get_mean_time(dates)

        result = []
        for msg, total_num, messages in aggregated_logs:
            data = {}
            data["message"] = msg
            data["total_logs"] = total_num
            data["timestamp"] = mean_time
            if messages:
                data["original_messages"] = messages
            result.append(data)

        return result

    def aggregator(self):
        logs_df, logs_json = self._get_logs_from_mg()
        if not logs_json:
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
        self._write_logs_to_mg(aggr_json)
        return aggr_json

