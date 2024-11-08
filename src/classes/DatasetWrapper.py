from utils.helper import extract_id_from_arn
import logging

class AWSDataset():

    def __init__(self, acc_id: str, user_arn:str, logger:logging):
        """Dataset Constructor
        Args:
            logger (object): logger object
            acc_id (str): aws account Id
            client (object): aws quicksight client
        """
        self.dataset_id = None
        self._acc_id = acc_id
        self._user_arn = user_arn
        self._logger = logger

    def set_dataset_id(self, dataset_id:str):
        self.dataset_id = dataset_id
    
    def describe_dataset(self, client, database_id:str, import_mode:str) -> dict[str,str]:
        '''Função Responsável por descrever as características de um dataset.
        Retorna um dicionário com os campos
        '''
        try:
            response = client.describe_data_set(
                AwsAccountId = self._acc_id, 
                DataSetId = database_id
            ).get('DataSet')

            self._logger.debug(response)

            dataset_info = {
                'Name': response.get('Name'),
                'DataSetId': response.get('DataSetId'),
                'PhysicalTableMap': response.get('PhysicalTableMap'),
                'LogicalTableMap': response.get('LogicalTableMap'),
                'ImportMode': import_mode if import_mode != 'DEFAULT' else response.get('ImportMode'),
                'Arn': response.get('Arn'),
                'DataSourceId': ''
            }

            # Caso PhysicalTableMap, armazenar o ID do datasource no campo DatasourceID do dicionário
            if len(dataset_info['PhysicalTableMap']):
                PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
                dataset_info['DataSourceId'] = extract_id_from_arn(dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'])

            return dataset_info

        except Exception as e:
            self._logger.error(f'An error ocurred in describe_dataset function: {e}')
            return None
        
    def create_dataset(self, client, dataset_info:dict[str]) -> int:
        ''' Função Responsável pela Criação de Datasets ea alteração de suas permissões na nova região'''
        try:
            response = client.create_data_set(
                AwsAccountId=self._acc_id,
                DataSetId=dataset_info['DataSetId'],
                Name=f'{dataset_info['Name']}_copy',
                PhysicalTableMap = dataset_info['PhysicalTableMap'],
                LogicalTableMap = dataset_info['LogicalTableMap'],
                ImportMode=dataset_info['ImportMode'],
                Permissions=[
                    {
                        'Principal': self._user_arn,
                        'Actions': ['quicksight:DescribeDataSet','quicksight:DescribeDataSetPermissions','quicksight:PassDataSet','quicksight:DescribeIngestion','quicksight:ListIngestions','quicksight:UpdateDataSet','quicksight:DeleteDataSet','quicksight:CreateIngestion','quicksight:CancelIngestion','quicksight:UpdateDataSetPermissions']

                    }
                ]
            )
            self._logger.info("Dataset Created Sucefully")
            return response
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
                self._logger.warning('Dataset Already Exists in This Region')
                return 2
            else:
                self._logger.error(f'An error ocurred in create_dataset function.\n Error: {e}')
            return 0

    def update_dataset(self, client, dataset_info):
        try:
            response = client.update_data_set(
                    AwsAccountId=self._acc_id,
                    DataSetId=dataset_info['DataSetId'],
                    Name=f'{dataset_info['Name']}_copy',
                    PhysicalTableMap = dataset_info['PhysicalTableMap'],
                    LogicalTableMap = dataset_info['LogicalTableMap'],
                    ImportMode=dataset_info['ImportMode'],
                    Permissions=[
                        {
                            'Principal': self._user_arn,
                            'Actions': ['quicksight:DescribeDataSet','quicksight:DescribeDataSetPermissions','quicksight:PassDataSet','quicksight:DescribeIngestion','quicksight:ListIngestions','quicksight:UpdateDataSet','quicksight:DeleteDataSet','quicksight:CreateIngestion','quicksight:CancelIngestion','quicksight:UpdateDataSetPermissions']

                        }
                    ]
            )
            self._logger.info("Dataset Created Sucefully")
            return response
        except Exception as e:
            self._logger.error(f'An error ocurred in create_dataset function.\n Error: {e}')
            return 0
        