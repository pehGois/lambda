import boto3
import logging

class AWSClient():
    
    def __init__(self, resource:str, source_info:dict, target_info, acc_id: str, logger:logging):
        self._target_client = boto3.client(resource, self._target_region)
        self._target_data_source_arn = target_info['arn']
        self._target_region = target_info['region-id']
        self._target_theme = target_info['theme']
        
        self._source_client = boto3.client(resource, self._source_region)
        self._source_data_source_arn = source_info['arn']
        self._source_region = source_info['region-id']
        self._source_theme = source_info['theme']

        self._acc_id = acc_id

        if not logger:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs.log')
            logger = logging.getLogger(__name__)

        else: self._logger = logger
    
    def get_source_region(self):
        return self._source_region
    
    def get_target_region(self):
        return self._target_region
    
    def get_source_client(self):
        return self._source_client
    
    def get_target_client(self):
        return self._target_client
    
    def get_source_theme(self):
        return self._source_theme
    
    def get_target_theme(self):
        return self._target_theme
    
    def get_account_id(self):
        return self._acc_id
    
    def get_logger(self):
        return self._logger
