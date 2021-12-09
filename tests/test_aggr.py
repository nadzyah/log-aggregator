"""Test mean time"""

#import sys
#sys.path.extend(['/home/nadzya/Apps/log-anomaly-detector/',
                 "/home/nadzya/Apps/log-aggregator"])

import pytest
import datetime
from pprint import pprint
import calendar

from aggregator.log_aggregator import Aggregator
from anomaly_detector.config import Configuration
from anomaly_detector.storage.storage_attribute import MGStorageAttribute

@pytest.fixture()
def config():
    """Initialize configurations before testing."""
    cfg = {"MG_HOST": "172.17.18.83",
           "MG_PORT": 27017,
           "MG_CERT_DIR": "./configs/LAD_CA.crt",
           "MG_INPUT_DB": "anomalydb",
           "MG_INPUT_COL": "utm_anomaly",
           "HOSTNAME_INDEX": "hostname",
           "DATETIME_INDEX": "timestamp",
           "LOGSOURCE_HOSTNAME": "172.17.31.10",
           "MG_USER": "dbadmin",
           "MG_PASSWORD": 'password123',
           "MODEL_DIR": "./models/"
           }

    cfg = Configuration(config_dict=cfg)
    storage_attr = MGStorageAttribute(86400*5, 10000)
    return cfg, storage_attr


def test_mean_time(config):
    """Test ability to get mean time"""
    cfg, mgstor_attr = config
    aggr = Aggregator(cfg)
    base = datetime.datetime.today()
    date_list = [base - datetime.timedelta(days=x) for x in range(9)]
    date_int_list = []
    for d in date_list:
        date_int_list.append(calendar.timegm(d.timetuple()))
    pprint(date_int_list)
    mean = aggr._get_mean_time(date_int_list)
    assert mean == date_list[4]
