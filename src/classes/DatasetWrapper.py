class DatasetWrapper():

    def __init__(self, logger: object, client: object, acc_id:str) -> None:
        """Analysis Constructor
        Args:
            logger (object): logger object
            acc_id (str): aws account Id
            client (object): aws quicksight client
        """
        self.logger = logger
        self.acc_id = acc_id
        self.client = client
        self.dataset_id = None
    
    def set_dataset_id(self, dataset_id:str):
        self.dataset_id = dataset_id
    
    def describe_dataset(client, acc_id:str, database_id:str) -> dict[str]:
        '''Função Responsável por descrever as características de um dataset.
        Retorna um dicionário com os campos
        '''
        try:
            response = client.describe_data_set(
                AwsAccountId = acc_id, 
                DataSetId = database_id
            )
            print("\n\n",response)
            response = response.get('DataSet')
            dataset_info = {
                'Name': response.get('Name'),
                'DataSetId': response.get('DataSetId'),
                'PhysicalTableMap': response.get('PhysicalTableMap'),
                'LogicalTableMap': response.get('LogicalTableMap'),
                'ImportMode': response.get('ImportMode'),
                'Arn': response.get('Arn')
            }

            if len(dataset_info['PhysicalTableMap']):
                PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
                dataset_info['DataSourceId'] = extract_id_from_arn(dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'])

            print("Retornou Certo: ", dataset_info) 
            return dataset_info

        except Exception as e:
            logger.error(f'An error ocurred in describe_dataset function: {e}')
            return None
        
    def create_dataset(client, acc_id:str, dataset_info:dict[str], user_arn:str) -> int:
        ''' Função Responsável pela Criação de Datasets ea alteração de suas permissões na nova região'''
        try:
            response = client.create_data_set(
                AwsAccountId=acc_id,
                DataSetId=dataset_info['DataSetId'],
                Name=f'{dataset_info['Name']}_copy',
                PhysicalTableMap = dataset_info['PhysicalTableMap'],
                LogicalTableMap = dataset_info['LogicalTableMap'],
                ImportMode=dataset_info['ImportMode'],
                Permissions=[
                    {
                        'Principal': user_arn,
                        'Actions': ['quicksight:DescribeDataSet','quicksight:DescribeDataSetPermissions','quicksight:PassDataSet','quicksight:DescribeIngestion','quicksight:ListIngestions','quicksight:UpdateDataSet','quicksight:DeleteDataSet','quicksight:CreateIngestion','quicksight:CancelIngestion','quicksight:UpdateDataSetPermissions']

                    }
                ]
            )
            logger.info("Dataset Created Sucefully")
            return response
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
                logger.warning('Dataset Already Exists in This Region')
                return 2
            else:
                logger.error(f'An error ocurred in create_dataset function.\n Error: {e}')
            return 0

    def update_dataset(client, acc_id,dataset_info, user_arn):
        try:
            response = client.update_data_set(
                    AwsAccountId=acc_id,
                    DataSetId=dataset_info['DataSetId'],
                    Name=f'{dataset_info['Name']}_copy',
                    PhysicalTableMap = dataset_info['PhysicalTableMap'],
                    LogicalTableMap = dataset_info['LogicalTableMap'],
                    ImportMode=dataset_info['ImportMode'],
                    Permissions=[
                        {
                            'Principal': user_arn,
                            'Actions': ['quicksight:DescribeDataSet','quicksight:DescribeDataSetPermissions','quicksight:PassDataSet','quicksight:DescribeIngestion','quicksight:ListIngestions','quicksight:UpdateDataSet','quicksight:DeleteDataSet','quicksight:CreateIngestion','quicksight:CancelIngestion','quicksight:UpdateDataSetPermissions']

                        }
                    ]
            )
            logger.info("Dataset Created Sucefully")
            return response
        except Exception as e:
            logger.error(f'An error ocurred in create_dataset function.\n Error: {e}')
            return 0
        