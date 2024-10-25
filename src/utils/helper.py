import re

def extract_id_from_arn(arn:str) -> str:
    ''' Função responsável por retorar de um ARN o ID do objeto '''
    pattern = r'(?<=/).*'
    try:
        return re.search(pattern, arn).group(0)
    except Exception:
        return ''
    
def switch_datasource_arn(self, dataset_info: dict) -> dict:
        """Switches the datasource Arn of the source analysis to the target one to enable migration."""

        if dataset_info['PhysicalTableMap']:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = self._target_data_source_arn

        return dataset_info