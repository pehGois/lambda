from utils.utils import logger, extract_id_from_arn
from classes.AnalysisWrapper import AWSAnalysis
from classes.DatasetWrapper import AWSDataset

def migrate_analysis_handler(acc_id: str, analysis_id: str, user_arn: str, source_client: dict, target_client: dict) -> int:
    """Handles the migration function and saves the .qs file into the S3."""
    try:
        analysis = AWSAnalysis(logger,acc_id,source_client)
        analysis_definition = analysis.describe_analysis_definition(analysis_id)
    
        arn_list_dict = analysis_definition['Definition']['DataSetIdentifierDeclarations']
        datasets = AWSDataset(logger,acc_id,source_client)

        for index, arn in enumerate(arn_list_dict):
            # Creating each dataset of the analysis in the new region
            dataset_id = extract_id_from_arn(arn['DataSetArn'])
            new_arn = create_dataset_handler(acc_id, dataset_id, user_arn, source_client, target_client)
            analysis_definition['Definition']['DataSetIdentifierDeclarations'][index]['DataSetArn'] = new_arn

        if analysis_definition.get('ThemeArn'):
            # If there's a theme in the analysis, replace then by the new region theme
            analysis_definition['ThemeArn'] = source_client['theme']

        return 1
    
    except Exception as e:
        logger.error(f'An error occurred in migrate_analysis_handler function.\nError: {e}')
        return 0
    
def update_template_handler(client, acc_id: str, analysis_id: str, comment: str) -> int:
    """Handles the template update."""
    try:
        analysis = AWSAnalysis(logger,acc_id,client)
        datasets = AWSDataset(logger,acc_id,client)
        analysis_info = analysis.describe_analysis(analysis_id)
        dataset_references = datasets.create_dataset_references(analysis_info['DataSetArns'])

        analysis.update_template(analysis_info, comment, dataset_references)
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



        return 1

    except Exception as e:
        logger.error(f'An error occurred in update_template_handler function.\nError: {e}')
        return 0