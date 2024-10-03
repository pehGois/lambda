import re
import logging
import requests
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs.log')
logger = logging.getLogger(__name__)
import traceback
import time
# ANALYSIS

def describe_analysis(client, acc_id:str, analysis_id: str) -> dict :
    """Describes the analysis details

    Args:
        client (class): quicksight client
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id

    Returns:
        dict: 
        Arn [str],
        Id [str],
        Name [str],
        DatasetArns [list],
        ThemeArn [str] 
    """
    try:
        analysis_descripion = client.describe_analysis(AwsAccountId=acc_id,AnalysisId=analysis_id)['Analysis']

        analysisInfo = {
            'Arn': analysis_descripion['Arn'],
            'Id': analysis_id,
            'Name': analysis_descripion['Name'],
            'DataSetArns': analysis_descripion['DataSetArns']
        }

        if 'ThemeArn' in analysis_descripion:
            analysisInfo['ThemeArn'] = analysis_descripion['ThemeArn']
        else: 
            analysisInfo['ThemeArn'] = None

        return analysisInfo
    except Exception as e:
        logger.error(f'An error ocurred in describe_analysis function.\n Error: {e}')

def update_analysis(client, acc_id:str, analysis_info:dict, template_info:dict, dataset_references:dict) -> int :
    """ Update the Analysis ID based in a template

    Args:
        client (class): quicksight client
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id
        template_info (dict): Template Dct
        dataset_references (dict): _description_

    Returns:
        int: _description_
    """
    try:
        client.update_analysis(
            AwsAccountId=acc_id,
            AnalysisId=analysis_info['Id'],
            Name=analysis_info['Name'],
            SourceEntity = {
                'SourceTemplate':{
                    'DataSetReferences': dataset_references,
                    'Arn': template_info['Arn']
                }
            }
        )
    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceNotFoundException'>":
            logger.error('Analysis is Not Found')
            return 2
        logger.error(f'An error ocurred in update_analysis function.\n Error: {e}')
        return 0
    else:
        logger.info('Analysis Updated Successfully')
        return 1

def create_analysis(client, acc_id:str, analysis_info:dict, template_info:dict, dataset_references:dict) -> str:
    """Create an Analysis in the given Region

    Args:
        client (class): quicksight client
        acc_id (str): account ID
        analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
        template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
        dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function

    Returns:
        str: _description_
    """
    try:
        client.create_analysis(
            AwsAccountId=acc_id,
            AnalysisId=analysis_info['Id'],
            Name=analysis_info['Name'],
            SourceEntity = {
                'SourceTemplate':{
                    'DataSetReferences': dataset_references,
                    'Arn': template_info['Arn']
                }
            }
        )
    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
            logger.error('Analysis already exists')
            return 2
        elif str(type(e)) == "<class 'botocore.errorfactory.ResourceNotFoundException'>":
            logger.error('Analysis is Not Found')
            return 0
        else:
            logger.error(f'An error ocurred in create_analysis function.\n Error: {e}')
            return 0
    else:
        logger.info('Analysis Created With Success')
        return 1

def list_analysis(client, acc_id: str, next_token=None, analyses=[]):
    try:
        if next_token:
            response = client.list_analyses(AwsAccountId=acc_id, NextToken=next_token)
        else:
            response = client.list_analyses(AwsAccountId=acc_id)

        for analysis in response['AnalysisSummaryList']:
            analyses.append({
                'Id': analysis['AnalysisId'],
                'Name': analysis['Name'],
                'Arn': analysis['Arn'],
                'Status': analysis['Status'],
                'CreatedTime': analysis['CreatedTime']
            })

        next_token = response.get('NextToken')
        if next_token:
            return list_analysis(client, acc_id, next_token, analyses)

    except Exception as e:
        logger.error(f"An error occurred in the list_analysis function.\nError: {e}")
        
    return analyses

def list_deleted_analysis(client, acc_id) -> list[dict[str]]:
    """Filter the Analysis whose status is equal to DELETED

    Args:
        client (class): quicksight client
        acc_id (str): account Id

    Returns:
        list[dict[str]]: Id, Name, Arn, Status 
    """
    try:    
        analysis_list = list_analysis(client, acc_id)
        data = [analysis for analysis in analysis_list if analysis['Status'] == 'DELETED']
        return data
    except Exception as e:
        logger.error(f"An error ocurred in list_deleted_analysis function.\n Error: {e}")

def restore_analysis(client, acc_id:str, analysis_id:str) -> int:
    """Restore a Deleted Analysis

    Args:
        client (class): quicksight client
        acc_id (str): account id
        analysis_id (str): deleted analysis id

    Returns:
        int: Returns 1 if Success and 0 if Fail  
    """
    try:
        logger.debug(client.restore_analysis(
            AwsAccountId=acc_id,
            AnalysisId=analysis_id
        ))
        logger.info("Analysis Restoured")
        return 1
    except Exception as e:
        logger.error(f"An error ocurred in restore_analysis function.\nError: {e}")
        return 0

def describe_analysis_definition(client, acc_id:str, analysis_id:str) -> dict:
    """Retorna um Dicionário Descrevendo a Análise em Detalhes

    Args:
        client (class): quicksight client
        acc_id (str): account id
        analysis_id (str): analysis id

    Returns:
        dict:
    """
    try: 
        logger.info("Describing analysis Definition")
        response = client.describe_analysis_definition(
            AwsAccountId=acc_id,
            AnalysisId=analysis_id
        )
        
        data ={
            'Definition': response['Definition'],
            'Name': response['Name'],
            'Id': response['AnalysisId']
        }

        if 'ThemeArn' in data:
            data['ThemeArn'] = response['ThemeArn']
        else: 
            data['ThemeArn'] = None
        return data
    except Exception as e:
        logger.error(f"An error ocurred in describe_analysis_definition function.\nError: {e}")
        return 0
    
def grant_auth(client, acc_id:str, analysis_id:str, user_arn:str):
    ''' Atualiza as Permissões de Um Usuário Sobre uma Análise: Update, Restore, Delete, Query e Describe'''
    try:
        client.update_analysis_permissions(
        AwsAccountId=acc_id,
        AnalysisId= analysis_id,
        GrantPermissions=[
                {
                    'Principal': user_arn,
                    'Actions': [
                            'quicksight:UpdateAnalysis',
                            'quicksight:RestoreAnalysis', 
                            'quicksight:UpdateAnalysisPermissions', 
                            'quicksight:DeleteAnalysis', 
                            'quicksight:QueryAnalysis',
                            'quicksight:DescribeAnalysisPermissions', 
                            'quicksight:DescribeAnalysis'
                        ]
                }
            ]
        )
        logger.info("Permission Granted Successfully")
    except Exception as e:
        logger.error(f'An error ocurred in grant_auth function.\n Error: {e}')

def create_analysis_by_definition(client, acc_id:str, analysis_definition:dict) -> int:
    """Create an Analysis in the given Region

    Args:
        client (class): quicksight client
        acc_id (str): account ID
        analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
        template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
        dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function

    Returns:
        int: 1 if Success, 2 If already Exists, 1 if Fail
    """
    try:
        client.create_analysis(
            AwsAccountId = acc_id,
            AnalysisId = analysis_definition['Id'],
            Name = analysis_definition['Name'],
            Definition = analysis_definition['Definition']
        )
    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
            logger.warning('Analysis Already Exists in this Region')
            return 2
        elif str(type(e)) == "<class 'botocorefactory.ResourceNotFoundException'>":
            logger.error('Analysis is Not Found')
            return 0
        else:
            logger.error(f'An error ocurred in create_analysis_by_definition function.\n Error: {e}')
            return 0
    else:
        logger.info('Analysis Created With Success')
        return 1

# DATASETS

def describe_dataset(client, acc_id:str, database_id:str) -> dict[str]:
    '''Função Responsável por descrever as características de um dataset.
    Retorna um dicionário com os campos
    '''
    try:
        response = client.describe_data_set(
            AwsAccountId = acc_id, 
            DataSetId = database_id
        ).get('DataSet')
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
    

# TEMPLATES

def create_template(client, acc_id: str, analysis_info: dict[str,dict], comment:str, dataset_references) -> int:
    '''Função Responsável por criar um template para o dataset ou análise especificados'''
    try:
        if not comment: comment ='Nenhuma Observação'
        SourceEntity = {
            'SourceAnalysis': {
                'Arn': analysis_info['Arn'],
                'DataSetReferences': dataset_references
            }
        }

        client.create_template(
            AwsAccountId=acc_id,
            TemplateId=analysis_info['Id'],
            Name=f'{analysis_info['Name']}_template',
            SourceEntity=SourceEntity,
            VersionDescription = comment
        )

    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
            logger.error('Template Already Exists')
            return 0
        else:
            logger.error(f'An error ocurred in create_template function. Analysis Info - \n {analysis_info['Name']}\n   {analysis_info['Id']}.   \n    Error: {e}')
    else:
        logger.error(f'Template creation for {analysis_info['Name']} was SUCESSFULL \n Template ID: {analysis_info['Id']}')
        return 1

def describe_template(client, acc_id:str, template_id:str, version:str = None):
    ''' Função Responsável por descrever um template. Retorna um Dicionário de Strings de campos: Arn, Id, Name, Version, Description'''
    try:
        if version:
            template = client.describe_template(
                AwsAccountId=acc_id,
                TemplateId=template_id,
                VersionNumber=int(version)
            )['Template']
        else:
            template = client.describe_template(
                AwsAccountId=acc_id,
                TemplateId=template_id
            )['Template']
        return {
            'Arn': template['Arn'],
            'Id': template['TemplateId'],
            'Name': template['Name'],
            'Version':  template['Version']['VersionNumber'],
            'Description': template['Version']['Description']
        }
    except Exception as e:
        logger.error(f'An error ocurred in describe_template function.\n Error: {e}')

def delete_template(client, acc_id:str, template_info:dict):
    ''' Função responsável por exluir um template '''
    try:
        client.delete_template(AwsAccountId=acc_id,
            TemplateId=template_info['Id'],
            VersionNumber=template_info['Version']
        )
    except Exception as e:
        logger.error(f'An error ocurred in delete_template function \n Error: {e}')

def update_template(client, acc_id:str, analysis_info:dict, comment:str, dataset_references:dict) -> int:
    try:
        if not comment: comment ='Nenhuma Observação'
        SourceEntity = {
            'SourceAnalysis': {
                'Arn': analysis_info['Arn'],
                'DataSetReferences': dataset_references
            }
        }

        client.update_template(
            AwsAccountId=acc_id,
            TemplateId=analysis_info['Id'],
            SourceEntity=SourceEntity,
            VersionDescription=comment,
            Name=f'{analysis_info['Name']}_template'
        )

    except Exception as e:
        logger.error(f'\nAn error ocurred in update_template function \n Error: {e}')
        return 0
    else:
        logger.info('Template Updated Sucefully')
        return 1

# USERS

def search_user(client, acc_id:str, email:str) -> dict:
    ''' Lista os Usuários e Retorna os Dados do Email Fornecido '''
    try:
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

# OTHERS

def extract_id_from_arn(arn:str) -> str:
    ''' Função responsável por retorar de um ARN o ID do objeto '''
    pattern = r'(?<=/).*'
    try:
        return re.search(pattern, arn).group(0)
    except Exception:
        return ''

def get_file(url, filename):
    ''' Faz uma requisição a uma URL e coleta o seu conteúdo em arquivo .qs '''
    try:
        response = requests.get(url)

        with open(f'{filename}.qs', 'wb') as file:
            file.write(response.content)
    except Exception as e:
        logger.error(f'An error ocurred in get_file function.\n Error: {e}')

def open_file(path):
    ''' Abre um arquivo e retorna o seu binário '''
    with open(path, 'rb') as file:    
        return file.read()
    
def create_dataset_references(client, acc_id:str, analysis_datasets_arns: list) -> dict[str,str]:
    """Function

    Args:
        client (class):  quicksight client
        acc_id (str): aws account id
        analysis_datasets_arns (list): list of the datasets arns from an analysis 

    Returns:
        dict[str,str]: DataSetPlaceholder, DataSetArn
    """

    try:
        DataSetReferences = []
        for arn in analysis_datasets_arns:
            database_info = describe_dataset(client, acc_id,extract_id_from_arn(arn))
            DataSetReferences.append(
            {
                'DataSetPlaceholder':database_info['Name'],
                'DataSetArn': arn
            })
        return DataSetReferences
    except Exception as e:
        logger.error(f'An error ocurred in create_dataset_references function.\n Error: {e}')