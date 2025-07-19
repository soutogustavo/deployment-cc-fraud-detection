import yaml


def load_config(config_path="config/fraud_configuration.yml"):
    """
    Load configuration from a YAML file.
    """
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
