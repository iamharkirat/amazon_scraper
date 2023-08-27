"""Load configuration from .yaml file."""
import yaml
import os.path as path


def yaml_loader(config_path=path.abspath(path.join(__file__, "../../..")) + "/config.yaml"):
    with open(config_path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config
