import re
import logging
import datetime

def get_logger():
    """ Return the Logger Object """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='src/logs/logs.log')
    return logging.getLogger(__name__)

def extract_id_from_arn(arn:str) -> str:
    ''' Função responsável por retorar de um ARN o ID do objeto '''
    pattern = r'(?<=/).*'
    try:
        return re.search(pattern, arn).group(0)
    except Exception:
        return ''
    
def switch_datasource_arn(dataset_info: dict, target_data_source_arn: str) -> dict:
        """Switches the datasource Arn of the source analysis to the target one to enable migration."""

        if dataset_info['PhysicalTableMap']:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = target_data_source_arn

        return dataset_info

def search_user(client, acc_id:str, email:str) -> dict:
    ''' Lista os Usuários e Retorna os Dados do Email Fornecido '''
    try:
        logger = get_logger()
        response = client.list_users(
        AwsAccountId=acc_id,
        Namespace = 'default'
        )['UserList']
        user = ''
        for user in response:
            if email == user['Email']:
                return user['Arn']
        raise ValueError ('Email does not exist in the database')
    except ValueError as e:
        logger.error(e)
    except Exception as e: 
        logger.error(f'An error ocurred in search_user function.\n Error: {e}')

def return_message(action, status, **kwargs) -> dict:
    with open("src/logs/logs.log", "r") as log: log = log.read().splitlines()
    with open("src/logs/logs.log", "w"): pass # Limpando o arquivo de log

    data = {
        "date":datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "action":action,
        "status":'SUCCESS' if status == 1 else 'FAIL',
        "logs": log
    }

    for key,value in kwargs.items():
        if key in kwargs:
            data[key] = value

    return data