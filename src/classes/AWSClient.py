import boto3
import logging

class AWSClient():
    def __init__(self, resource:str, region:str,acc_id: str, logger:logging) -> None:
        self._client = boto3.client(resource, region_name=region)
        if not logger:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs.log')
            logger = logging.getLogger(__name__)
        else: self._logger = logger
        self._acc_id = acc_id
