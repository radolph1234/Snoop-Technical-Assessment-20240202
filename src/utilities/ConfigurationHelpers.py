import os


def get_lake_location() -> str:
    """
    Get the location of the lake from an environment variable.

    :return: Location of raw data
    :rtype: str
    """
    
    env = os.environ.get("environment")

    if env == 'local':
        return 'src\datalake'