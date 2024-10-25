import boto3

class AWSClientHandler():
    
    def __init__(self, resource:str, source_info:dict, target_info):
        self._target_data_source_arn = target_info['data_source_arn']
        self._target_region = target_info['region-id']
        self._target_theme = target_info['theme']
        self._target_client = boto3.client(resource, self._target_region)
        
        self._source_data_source_arn = source_info['data_source_arn']
        self._source_region = source_info['region-id']
        self._source_theme = source_info['theme']
        self._source_client = boto3.client(resource, self._source_region)

        # self._acc_id = acc_id
        # self._user_arn = user_arn
    
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
