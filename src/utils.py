import logging
import json
import requests
import re
import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='logs.log')
logger = logging.getLogger(__name__)

# Handlers
def s3_save_json(data:dict):
    """This function constructs a dict to send to the S3 Bucket and saves it in a temp JSON file"""
    try:
        file_path = 's3_response.json'
        with open(file_path, 'w', encoding='UTF-8') as file:
            json.dump(data, file, indent=4, default=str, ensure_ascii=False)
        return file_path
    except Exception as e:
        logger.error(f'\nAn error ocurred in s3_save_json function\n   Error: {e}')

def s3_upload_file(s3_client, data:dict[str,str,str], file_path:str, bucket_name:str, stakeholder:str):
    """Function that upload the data to a S3 bucket in AWS

    Args:
        s3_client : client s3
        data (dict): dict with the analysis/template info
        file_path (str): path where the data is saved
        bucket_name (str): the name of the bucket in AWS
        stakeholder (str): the name of the stakeholder. This will act as a folder inside the Bucket
    """
    object_name = f'{data['name']}_{data['version'] if data['version'] != 0 else "migration"}_{data['date']}.{file_path.split(".")[1]}'
    try:
        path = f'quicksight_templates/{stakeholder.upper()}/{data['name'].replace('_template',"").lower()}/{object_name}'
        s3_client.upload_file(file_path, bucket_name, path)
        logger.debug(f'File {data} uploaded to {bucket_name} on the path:{path}')
        logger.info(f"Data uploaded Sucessfully to {bucket_name} on the path: {path}")
    except FileNotFoundError:
        logger.error('The file was not found')
    except Exception as e:
        logger.error(f'An error ocorred in s3_upload_file function.\nError Message:{e}')

def migrate_analysis_handler(acc_id:str, analysis_id:str, user_arn:str, source_client:dict, target_client:dict, s3_client, bucket_name, stakeholder) -> int:
    """Handle the migration function and saves the .qs file into the s3

    Args:
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id
        user_arn (str): User ARN. Can be obtained by Search User Function
        source_client (dict): Client where the analysis already is
        target_client (dict): Client that you want to transfer the analysis to
        s3_client (class): S3 Client to save the analysis definition

    Returns:
        int: Returns 1 if 'SUCCESS' and 0 if 'FAIL'
    """
    try:
        analysis_definition = describe_analysis_definition(source_client['client'], acc_id, analysis_id)
        arn_list = analysis_definition['Definition']['DataSetIdentifierDeclarations']
        
        for index, dataset_identifier in enumerate(arn_list):
            analysis_definition['Definition']['DataSetIdentifierDeclarations'][index]['DataSetArn'] = create_dataset_handler(acc_id, extract_id_from_arn(dataset_identifier['DataSetArn']), user_arn, source_client, target_client)

        if analysis_definition['ThemeArn'] : analysis_definition['ThemeArn'] = source_client['theme']

        create_analysis_by_definition(target_client['client'],acc_id,analysis_definition)
        grant_auth(target_client['client'],acc_id,analysis_id,user_arn)
        
        info = {
            'author': user_arn.split("/")[2],
            'source_region': source_client['region'], 
            'template_id': analysis_definition['Id'],
            'name': analysis_definition['Name'],
            'date': datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
            'analysis_definition': analysis_definition,
            'version': 0
        }

        file_path = s3_save_json(info)

        s3_upload_file(s3_client, info, file_path, bucket_name, stakeholder)
        
        return 1
    
    except Exception as e:
        logger.error(f'An error ocurred in migrate_analysis_handler function.\n Error: {e}')
        return 0

def update_analysis_handler(client, acc_id:str, analysis_id:str, version:str, user_arn:str):
    """Handle the update of the analysis

    Args:
        client: quickstart client
        acc_id (str): account Id
        analysis_id (str): analysis id
        version (str): version of the template that you want to update the analysis for
        user_arn (str): user arn

    Returns:
        int: 0 for failure and 1 for success
    """
    try:
        analysis_info = describe_analysis(client,acc_id,analysis_id)
        template_info = describe_template(client, acc_id, analysis_id, version)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])
        
        if update_analysis(client, acc_id, analysis_info, template_info, dataset_references) == 2:
            logger.info("Starting to Recreate the Analysis Based in the Template")
            create_analysis(client, acc_id, analysis_info,template_info,dataset_references)
            grant_auth(client, acc_id, analysis_info['Id'],user_arn)
        return 1
    except Exception as e:
        logger.error(f'An error ocorred in update_analysis_handler function.\nError Message:{e}')
        return 0


def create_dataset_handler(acc_id:str, database_id:str, user_arn:str, source_client: dict, target_client: dict) -> int:
    """Handle the dataset Creation

    Args:
        acc_id (str): quicksight account
        database_id (str): Database Identifier
        user_arn (str): User ARN. Get by search User Function
        source_client (dict): Client where the dataset already is
        target_client (dict): Client that you want to transfer the dataset to

    Returns:
        int: Returns 1 if 'SUCCESS' and 0 if 'FAIL'
    """
    def switch_arn(dataset_info:dict):
        """Function that switches the datasource Arn of the source analysis to the target one to make possible the migration."""
        #Processo de Substituição do ARN do banco para o ARN da região Target
        if len(dataset_info['PhysicalTableMap']) > 0:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = target_client['arn']
        return dataset_info
    try:
        dataset_info = describe_dataset(client=source_client['client'], acc_id=acc_id, database_id=database_id)
        
        if len(dataset_info['LogicalTableMap']) > 0 and len(dataset_info['PhysicalTableMap']) == 0:
            logical_table = dataset_info['LogicalTableMap']

            for id, logical_table_info in logical_table.items():
                if logical_table_info['Alias'] != 'Intermediate Table':
                    join_dataset_info = describe_dataset(source_client['client'], acc_id, extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                    join_dataset_info = switch_arn(join_dataset_info)

                    create_dataset(client=target_client['client'], acc_id=acc_id, user_arn=user_arn, dataset_info=join_dataset_info)
                    dataset_info['LogicalTableMap'][id]['Source']['DataSetArn'] = logical_table_info['Source']['DataSetArn'].replace(source_client['region'],target_client['region'])
        
        else: dataset_info = switch_arn(dataset_info)

        response = create_dataset(client=target_client['client'], acc_id=acc_id, user_arn=user_arn, dataset_info=dataset_info)
        
        if response == 2:
            return dataset_info['Arn'].replace(source_client['region'],target_client['region'])
        return response['Arn']
    except Exception as e:
        logger.error(f'An error ocorred in create_dataset_handler function.\nError Message:{e}')
        return 0

def create_template_handler(client,acc_id:str,analysis_id:str, comment:str, email:str, s3_client, bucket_name:str, stakeholder:str) -> int:
    """Handle the Template Creation

    Args:
        client (class): quicksight client
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id
        user_arn (str): User ARN. Can be obtained by Search User Function
        s3_client (class): s3 client
        bucket_name (str): Name of the S3 bucket where the teamplate file will be stored
        stakeholder (str): Name of the Stakeholder of the Template
        version (string, optional): Version of the Template. Defaults to None.

    Returns:
        int: Returns 1 if Success and 0 if Fail
    """
    try: 
        analysis_info = describe_analysis(client,acc_id,analysis_id)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])
        if not create_template(client, acc_id, analysis_info, comment, dataset_references):
            return 2
        template_info = describe_template(client,acc_id,analysis_id)
        analysis_definition = describe_analysis_definition(client, acc_id, analysis_id)
        
        info = {
            'author': email,
            'source_region': template_info['Arn'].split(':')[3], 
            'template_id': template_info['Id'],
            'name': template_info['Name'],
            'date': datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
            'analysis_definition': analysis_definition,
            'version' : template_info['Version'],
            'comment' : template_info['Description']
        }

        file_path = s3_save_json(info)
        s3_upload_file(s3_client,info,file_path,bucket_name,stakeholder)         
        logger.info('Template Created Sucefully')
        return 1

    except Exception as e: 
        logger.error(f'An error ocurred in create_analysis_template function. \n    Error: {e}')
        return 0

def update_template_handler(client,acc_id:str,analysis_id:str,comment:str, email:str, s3_client, bucket_name:str, stakeholder:str) -> int:
    """Handle the Template Update

    Args:
        client (class): quicksight client
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id
        user_arn (str): User ARN. Can be obtained by Search User Function
        s3_client (class): s3 client
        bucket_name (str): Name of the S3 bucket where the template file will be stored
        stakeholder (str): Name of the Stakeholder of the Template
        version (string, optional): Version of the Template. Defaults to None.

    Returns:
        int: Returns 1 if Success and 0 if Fail
    """
    try:
        analysis_info = describe_analysis(client,acc_id,analysis_id)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])

        update_template(client, acc_id, analysis_info, comment, dataset_references)
        template_info = describe_template(client, acc_id, analysis_id)
        analysis_definition = describe_analysis_definition(client, acc_id, analysis_id)

        info = {
            'author': email,
            'source_region': template_info['Arn'].split(':')[3], 
            'template_id': template_info['Id'],
            'name': template_info['Name'],
            'date': datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
            'analysis_definition': analysis_definition,
            'version' : template_info['Version'],
            'comment' : template_info['Description']
        }

        file_path = s3_save_json(info)
        s3_upload_file(s3_client,info,file_path,bucket_name,stakeholder)   
        return 1

    except Exception as e:
        logger.error(f'An error ocurred in update_analysis_template function. \n    Error: {e}')
        return 0
    
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

def list_analysis(client, acc_id:str)-> list[dict[str]]:
    """List all the analysis in a region and returns it's characteristics 

    Args:
        client (class): quicksight client
        acc_id (str): account Id

    Returns:
        list[dict[str]]: Id, Name, Arn, Status 
    """
    try:
        list_analyses = []
        response = client.list_analyses(AwsAccountId=acc_id)['AnalysisSummaryList']
        for analysis in response:
            list_analyses.append({
                'Id':analysis['AnalysisId'],
                'Name': analysis['Name'],
                'Arn': analysis['Arn'],
                'Status': analysis['Status']
            })
        return list_analyses
    except Exception as e:
        logger.error(f"An error ocurred in list_analysis function.\n Error: {e}")

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

# DATASETS

def describe_dataset(client, acc_id:str, database_id:str) -> dict[str]:
    '''Função Responsável por descrever as características de um dataset.
    Retorna um dicionário com os campos
    {\n
        Name [str] ,
        DatasetId [str] ,
        PhysicalTableMap [dict] ,
        LogicalTableMap [dict] ,
        ImportMode [str] ,
        DataSourceId [str]
    }
    '''
    try:
        response = client.describe_data_set(
            AwsAccountId = acc_id, 
            DataSetId = database_id
        )['DataSet']

        dataset_info = {
            'Name': response['Name'],
            'DataSetId': response['DataSetId'],
            'PhysicalTableMap': response['PhysicalTableMap'],
            'LogicalTableMap': response['LogicalTableMap'],
            'ImportMode': response['ImportMode'],
            'Arn': response['Arn']

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
        print('Analysis Created With Success')
        return 1

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
        exit()
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

# ASSET_BUNDLE (Substituído por import via analysis describe definition)

#def export_asset_bundle(client, acc_id:str, analysis_info:dict[str,list]) -> str:
    ''' Função responsável por exportar uma ou um conjunto de análises em um arquivo .qs para posterior importação.\n Retorna ID do Export '''
    """ try:
        job_id = f'{analysis_info['Id']}_job_id'
        
        ResourceArns = []
        ResourceArns.append(analysis_info['Arn'])
        ResourceArns.append(analysis_info['ThemeArn'])

        client.start_asset_bundle_export_job(
        AwsAccountId=acc_id,
        AssetBundleExportJobId=job_id,
        ResourceArns=ResourceArns,
        IncludeAllDependencies=False,
        ExportFormat='QUICKSIGHT_JSON',

        IncludePermissions=True,
        IncludeTags=True,
        ValidationStrategy={
            'StrictModeForAllResources': True
        }
        )
        return job_id
    
    except Exception as e:
        logger.error(f'An error ocurred in export_asset_bundle function.\n Error: {e}')
        return None """

#def import_asset_bundle(client,acc_id:str,job_id:str, analysis_info:dict, user_arn:str) -> int:
    ''' Função Responsável por Importar o arquivo .qs na região desejada e atualizar as suas permissões de visualização na nova região.'''
    """ try:
        client.start_asset_bundle_import_job(
            AwsAccountId=acc_id,
            AssetBundleImportJobId=job_id,
            AssetBundleImportSource={
                'Body': open_file(f"{job_id}.qs")
            },
            FailureAction='ROLLBACK',     
            OverridePermissions={
                'Analyses': [
                    {
                        'AnalysisIds': [
                            analysis_info['Id'],
                        ],
                        'Permissions': {
                            'Principals': [
                                user_arn,
                            ],
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
                    },
                ]
            }
        )
        return 1
    except Exception as e:
        logger.error(f'An error ocurred in import_asset_bundle function when import an analysis with the following ID {analysis_info['Id']}.\n Error: {e}')
        return 0 """
    

#def describe_asset_bundle_export(client, acc_id: str, job_id: str) -> str:
    '''Monitor the export status of asset bundles and download the file upon completion.'''
    """ try:
        while True:
            response = client.describe_asset_bundle_export_job(
                AwsAccountId=acc_id,
                AssetBundleExportJobId=job_id
            )

            job_status = response.get('JobStatus')
            if job_status == 'IN_PROGRESS':

                logger.info('Export in Progress ...')
                time.sleep(5)
            else:
                break

        if job_status not in ['SUCCESSFUL', 'QUEUED_FOR_IMMEDIATE_EXECUTION']:
            errors = response.get('Errors')
            raise Exception (f'\n{[f'>> {error['Message']}' for error in errors]}')
        else:
            time.sleep(10)
            get_file(response['DownloadUrl'], job_id)
            return f'Export Status: {job_status}'
    
    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceNotFoundException'>":
            logger.error('Export Not Found')

        else:
            logger.error(f'An error ocurred in describe_asset_bundle_export function.\n Error: {e}') """

#def describe_asset_bundle_import(client, acc_id:str, job_id:str) -> str:
    ''' Função Responsável por monitorar o estado da importação do arquivo .qs '''
    """ try:
        while True:

            response = client.describe_asset_bundle_import_job(
                AwsAccountId= acc_id,
                AssetBundleImportJobId= job_id
            )

            job_status = response.get('JobStatus')
            if job_status == 'IN_PROGRESS':

                logger.info('Import in Progress ...')
                time.sleep(5)
            else:
                break
        try:
            return f'Import Status: {response['JobStatus']}\nError: {response['Errors'][0]['Message']}'
        except Exception:
            return f'Import Status: {response['JobStatus']}'
   
    except Exception as e:
        if str(type(e)) == "<class 'botocore.errorfactory.ResourceNotFoundException'>":
            logger.error('Import Not Found')
        else:
            logger.error(f'An error ocurred in describe_asset_bundle_import function.\n Error: {e}')
            return None """

#def migrate_asset_handler(acc_id:str, analysis_id:str, user_arn:str, source_client:dict, target_client:dict, s3_client, bucket_name:str, stakeholder:str) -> int:
    """Handle the migration function and saves the .qs file into the s3

    Args:
        acc_id (str): quicksight account
        analysis_id (str): Analysis Id
        user_arn (str): User ARN. Can be obtained by Search User Function
        source_client (dict): Client where the asset already is
        target_client (dict): Client that you want to transfer the asset to
        s3_client (class): s3 client
        bucket_name (str): Name of the S3 bucket where the .qs file will be stored
        stakeholder (str): Name of the Stakeholder of the Assets

    Returns:
        int: Returns 1 if 'SUCCESS' and 0 if 'FAIL'
    """
    """ try:
        analysis_info = describe_analysis(source_client['client'],acc_id,analysis_id)

        for dataset_arn in analysis_info['DataSetArns']:
            create_dataset_handler(acc_id, extract_id_from_arn(dataset_arn), user_arn, source_client, target_client)
        
        job_id = export_asset_bundle(source_client['client'], acc_id, analysis_info)
        time.sleep(5)
        logger.info(describe_asset_bundle_export(source_client['client'],acc_id,job_id))

        data = {"name":"asset_bundle", "version": 1, "date":datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}
        file_path = f"{job_id}.qs"
        s3_upload_file(s3_client,data,file_path,bucket_name,stakeholder)
        if import_asset_bundle(target_client['client'], acc_id, job_id, analysis_info, user_arn):
            time.sleep(5)
            logger.info(describe_asset_bundle_import(target_client['client'],acc_id,job_id))
            return 1
        raise Exception 
    
    except Exception as e:
        logger.error(f'An error ocurred in migrate_asset_handler function.\n Error: {e}')
        return 0 """