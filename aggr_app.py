import sys
sys.path.extend(["/home/nadzya/Apps/log-anomaly-detector/"])

"""Log aggregator"""
from aggregator.log_aggregator import Aggregator
from anomaly_detector.config import Configuration

import os
import click
import yaml

def get_configs(path_to_yaml_file):
    """Initialize configuration object from the yaml file"""
    configs = []
    with open(path_to_yaml_file, 'r') as f:
        yaml_data = yaml.safe_load(f)
        config_data = yaml_data.copy()
        if "MG_INPUT_COLS" in config_data.keys():
            del config_data["MG_INPUT_COLS"]
            del config_data["MG_TARGET_COLS"]
            for i in range(len(yaml_data["MG_INPUT_COLS"])):
                config_data["MG_INPUT_COL"] = yaml_data["MG_INPUT_COLS"][i]
                config_data["MG_TARGET_COL"] = yaml_data["MG_TARGET_COLS"][i]
                configs.append(Configuration(config_dict=config_data))
        elif "MYSQL_INPUT_TABLES" in config_data.keys():
            del config_data["MYSQL_INPUT_TABLES"]
            del config_data["MYSQL_TARGET_TABLES"]
            for i in range(len(yaml_data["MYSQL_INPUT_TABLES"])):
                config_data["MYSQL_INPUT_TABLE"] = yaml_data["MYSQL_INPUT_TABLES"][i]
                config_data["MYSQL_TARGET_TABLE"] = yaml_data["MYSQL_TARGET_TABLES"][i]
                configs.append(Configuration(config_dict=config_data))
    return configs

@click.group()
def cli():
    return

@cli.command("run")
@click.option("--config-yaml", default="aggregator.yaml", help="configuration file used to configure service")
def run(config_yaml):
    configs = get_configs(config_yaml)
    for config in configs:
        aggr = Aggregator(config)
        aggr.aggregator()

if __name__ == "__main__":
    cli()
