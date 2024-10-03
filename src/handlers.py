import json
import datetime
from utils import *

# Handlers
def return_log_message(action, email, source, target = None, result = None, analysis_id = None, comment = None) -> dict:
    with open("logs.log", "r") as log: log = log.read().splitlines()
    with open("logs.log", "w"): pass # Limpando o arquivo de log

    return {
        "date":datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "action":action,
        "user":email,
        "source_region":source,
        "target_region":target,
        "status":'SUCCESS' if result == 1 else 'FAIL',
        "analysis_id":analysis_id,
        "comment":comment if comment else "Nenhuma Observação",
        "logs": log
    }

def return_json_message(body:str, email:str) -> dict:
    return {
        "date":datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "statusCode": "FAIL",
        "body": body,
        "user": email
    }

def s3_save_json(data: dict) -> str:
    """Constructs a dict to send to the S3 Bucket and saves it in a temp JSON file."""
    try:
        file_path = 's3_response.json'
        with open(file_path, 'w', encoding='UTF-8') as file:
            json.dump(data, file, indent=4, default=str, ensure_ascii=False)
        return file_path
    except Exception as e:
        logger.error(f'An error occurred in s3_save_json function\nError: {e}')

def s3_upload_file(s3_client, data: dict[str,str], file_path: str, bucket_name: str, stakeholder: str):
    """Uploads the data to an S3 bucket in AWS."""
    object_name = f"{data['name']}_{data['version'] if data['version'] != 0 else 'migration'}.json"
    try:
        path = f"quicksight_templates/{stakeholder.upper() if stakeholder else "OMOTOR"}/{data['name'].replace('_template', '').lower()}/{object_name}"
        s3_client.upload_file(file_path, bucket_name, path)
        logger.debug(f'File {data} uploaded to {bucket_name} on the path: {path}')
        logger.info(f"Data uploaded successfully to {bucket_name} on the path: {path}")
    except FileNotFoundError:
        logger.error('The file was not found')
    except Exception as e:
        logger.error(f'An error occurred in s3_upload_file function.\nError Message: {e}')

def create_metadata(email: str, template_info: dict, analysis_definition: dict, datasets_definition: list, comment: str = None) -> dict:
    """Create metadata for the template or analysis."""
    return {
        'author': email,
        'source_region': template_info['Arn'].split(':')[3] if 'Arn' in template_info else template_info['region'],
        'template_id': template_info['Id'],
        'name': template_info['Name'],
        'date': datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        'analysis_definition': analysis_definition,
        'dataset_definition': datasets_definition,
        'version': template_info.get('Version', 0),
        'comment': comment or template_info.get('Description', '')
    }

def save_metadata(info: dict, s3_client, file_path: str, bucket_name: str, stakeholder: str):
    """Save metadata to S3."""
    s3_upload_file(s3_client, info, file_path, bucket_name, stakeholder)

def handle_s3_upload(info: dict, s3_client, bucket_name: str, stakeholder: str):
    """Create JSON and upload it to S3."""
    file_path = s3_save_json(info)
    save_metadata(info, s3_client, file_path, bucket_name, stakeholder)

def migrate_analysis_handler(acc_id: str, analysis_id: str, user_arn: str, source_client: dict, target_client: dict, s3_client, bucket_name: str, stakeholder: str) -> int:
    """Handles the migration function and saves the .qs file into the S3."""
    try:
        print("Iniciou a Migração")
        analysis_definition = describe_analysis_definition(source_client['client'], acc_id, analysis_id)
        arn_list_dict = analysis_definition['Definition']['DataSetIdentifierDeclarations']

        for index, dataset_identifier in enumerate(arn_list_dict):
            # Creating each dataset of the analysis in the new region
            new_arn = create_dataset_handler(acc_id, extract_id_from_arn(dataset_identifier['DataSetArn']), user_arn, source_client, target_client)
            analysis_definition['Definition']['DataSetIdentifierDeclarations'][index]['DataSetArn'] = new_arn

        if analysis_definition.get('ThemeArn'):
            analysis_definition['ThemeArn'] = source_client['theme']
        
        print("Começou a Descrever o Dataset")
        
        datasets_definition = [describe_dataset(target_client['client'], acc_id, extract_id_from_arn(arn['DataSetArn'])) for arn in arn_list_dict]
        print(datasets_definition)

        create_analysis_by_definition(target_client['client'], acc_id, analysis_definition)
        grant_auth(target_client['client'], acc_id, analysis_id, user_arn)
        
        analysis_definition['region'] = source_client['region']
        info = create_metadata(user_arn.split("/")[2], analysis_definition, analysis_definition, datasets_definition, "Migração")
        handle_s3_upload(info, s3_client, bucket_name, stakeholder)

        return 1

    except Exception as e:
        logger.error(f'An error occurred in migrate_analysis_handler function.\nError: {e}')
        return 0

def update_template_handler(client, acc_id: str, analysis_id: str, comment: str, email: str, s3_client, bucket_name: str, stakeholder: str) -> int:
    """Handles the template update."""
    try:
        analysis_info = describe_analysis(client, acc_id, analysis_id)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])

        update_template(client, acc_id, analysis_info, comment, dataset_references)
        template_info = describe_template(client, acc_id, analysis_id)
        analysis_definition = describe_analysis_definition(client, acc_id, analysis_id)
        datasets_definition = [describe_dataset(client, acc_id, extract_id_from_arn(arn)) for arn in analysis_info['DataSetArns']]

        for dataset in datasets_definition:
            print(type(dataset), 'LogicalTableMap' in dataset)
            print(dataset)
            if 'LogicalTableMap' in dataset:
                if dataset['LogicalTableMap'] and not dataset['PhysicalTableMap']:
                    logical_table = dataset['LogicalTableMap']
                    for id, logical_table_info in logical_table.items():
                        # Intermediate Table is the table were the JOIN happens
                        if logical_table_info['Alias'] != 'Intermediate Table':

                            datasets_definition.append({
                                id:describe_dataset(client, acc_id, extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                            })

        info = create_metadata(email, template_info, analysis_definition, datasets_definition, comment)
        handle_s3_upload(info, s3_client, bucket_name, stakeholder)

        return 1

    except Exception as e:
        logger.error(f'An error occurred in update_template_handler function.\nError: {e}')
        return 0

def create_dataset_handler(acc_id: str, database_id: str, user_arn: str, source_client: dict, target_client: dict) -> int:
    """Handles the dataset creation."""

    def switch_arn(dataset_info: dict) -> dict:
        """Switches the datasource Arn of the source analysis to the target one to enable migration."""

        if dataset_info['PhysicalTableMap']:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = target_client['arn']

        return dataset_info

    try:
        dataset_info = describe_dataset(client=source_client['client'], acc_id=acc_id, database_id=database_id)
        
        #Verificando se é um Dataset de JOIN
        if dataset_info['LogicalTableMap'] and not dataset_info['PhysicalTableMap']:
            logical_table = dataset_info['LogicalTableMap']

            for id, logical_table_info in logical_table.items():
                # Intermediate Table is the table were the JOIN happens
                if logical_table_info['Alias'] != 'Intermediate Table':
                    join_dataset_info = describe_dataset(source_client['client'], acc_id, extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                    join_dataset_info = switch_arn(join_dataset_info)

                    create_dataset(client=target_client['client'], acc_id=acc_id, user_arn=user_arn, dataset_info=join_dataset_info)
                    dataset_info['LogicalTableMap'][id]['Source']['DataSetArn'] = logical_table_info['Source']['DataSetArn'].replace(source_client['region'], target_client['region'])
        
        else:
            dataset_info = switch_arn(dataset_info)

        response = create_dataset(client=target_client['client'], acc_id=acc_id, user_arn=user_arn, dataset_info=dataset_info)
        
        if response == 2:
            return dataset_info['Arn'].replace(source_client['region'], target_client['region'])
        return response['Arn']
    except Exception as e:
        logger.error(f'An error occurred in create_dataset_handler function.\nError Message: {e}')
        return 0

def create_template_handler(client, acc_id: str, analysis_id: str, comment: str, email: str, s3_client, bucket_name: str, stakeholder: str) -> int:
    """Handles the template creation."""
    try:
        # Describe analysis and gather required information
        analysis_info = describe_analysis(client, acc_id, analysis_id)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])

        # Attempt to create the template, return 2 if it fails
        if not create_template(client, acc_id, analysis_info, comment, dataset_references):
            return 2

        # Gather additional data to build the template metadata
        template_info = describe_template(client, acc_id, analysis_id)
        analysis_definition = describe_analysis_definition(client, acc_id, analysis_id)
        datasets_definition = [
            describe_dataset(client, acc_id, extract_id_from_arn(arn)) 
            for arn in analysis_info['DataSetArns']
        ]

        # Create metadata and upload to S3
        info = create_metadata(email, template_info, analysis_definition, datasets_definition, comment)
        handle_s3_upload(info, s3_client, bucket_name, stakeholder)

        logger.info('Template created successfully')
        return 1

    except Exception as e:
        logger.error(f'An error occurred in create_template_handler function.\nError: {e}')
        return 0
    
def update_analysis_handler(client, acc_id: str, analysis_id: str, version: str, user_arn: str) -> int:
    """Handles the update of the analysis."""
    try:
        analysis_info = describe_analysis(client, acc_id, analysis_id)
        template_info = describe_template(client, acc_id, analysis_id, version)
        dataset_references = create_dataset_references(client, acc_id, analysis_info['DataSetArns'])
        
        if update_analysis(client, acc_id, analysis_info, template_info, dataset_references) == 2:
            logger.info("Starting to recreate the analysis based on the template")
            create_analysis(client, acc_id, analysis_info, template_info, dataset_references)
            grant_auth(client, acc_id, analysis_info['Id'], user_arn)
        
        return 1
    except Exception as e:
        logger.error(f'An error occurred in update_analysis_handler function.\nError Message: {e}')
        return 0