import os


def get_lake_location():
    env = os.environ.get("environment")

    if env == 'local':
        return 'src\datalake'