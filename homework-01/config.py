import yaml


def get_config(config_file="config.yaml"):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)


if __name__ == "__main__":
    config = get_config()
    print(config)
