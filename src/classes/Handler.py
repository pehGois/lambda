from utils.utils import extract_id_from_arn
from classes.AnalysisWrapper import AWSAnalysis
from classes.DatasetWrapper import AWSDataset
from classes.AWSClientWrapper import AWSClientHandler
from classes.TemplatesWrapper import AWSTemplates
import logging

class Handler(AWSClientHandler):
    """ Service Responsable for Handling The Classes used in the Analysis Migration """
    def __init__(self, source_info:dict, target_info:dict, aws_analysis:AWSAnalysis, aws_dataset:AWSDataset, aws_template:AWSTemplates, logger:logging):
        self._aws_analysis = aws_analysis
        self._aws_datasets = aws_dataset
        self._aws_templates = aws_template
        self._logger = logger
        super(Handler, self).__init__('quicksight', source_info, target_info)

    def invoke(self, action, **kwargs):
        match action:
            case 'MIGRATION':
                return self.migrate_analysis_handler(kwargs.get('analysis_id'))
            case 'LIST_DELETED_ANALYSIS':
                return self._aws_analysis.list_deleted_analysis(self._target_client)
            case 'TEMPLATE_CREATION':
                return self.create_template_handler(kwargs.get('analysis_id'), kwargs.get('comment'))
            case 'ANALYSIS_UPDATE':
                return self.update_analysis_handler(kwargs.get('analysis_id'), kwargs.get('version'))
            case 'TEMPLATE_UPDATE':
                return self.update_template_handler(kwargs.get('analysis_id'), kwargs.get('comment'))
            case _:
                return 0

    # TODO Add to Helper        
    def _switch_datasource_arn(self, dataset_info: dict) -> dict:
        """Switches the datasource Arn of the source analysis to the target one to enable migration."""

        if dataset_info['PhysicalTableMap']:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = self._target_data_source_arn

        return dataset_info
    
    def create_dataset_references(self, client, analysis_datasets_arns: list) -> dict[str,str]:
        """Create a List with the names and ARN with all the datasets of a Analysis"""
        try:
            DataSetReferences = []
            for arn in analysis_datasets_arns:
                database_info = self._aws_datasets.describe_dataset(client, extract_id_from_arn(arn))
                DataSetReferences.append(
                {
                    'DataSetPlaceholder':database_info['Name'],
                    'DataSetArn': arn
                })
            return DataSetReferences
        
        except Exception as e:
            self._logger.error(f'An error ocurred in create_dataset_references function.\n Error: {e}')
            
    def migrate_analysis_handler(self, analysis_id:str) -> int:
        try:
            analysis_definition = self._aws_analysis.describe_analysis_definition(self._source_client, analysis_id)
            arn_list_dict = analysis_definition['Definition']['DataSetIdentifierDeclarations']

            # Creating each dataset of the analysis in the new region
            for i, arn in enumerate(arn_list_dict):
                dataset_id = extract_id_from_arn(arn['DataSetArn'])
                new_arn = self.create_dataset_handler(dataset_id)
                analysis_definition['Definition']['DataSetIdentifierDeclarations'][i]['DataSetArn'] = new_arn

            # If there's a theme in the analysis, replace then by the new region theme
            if analysis_definition.get('ThemeArn'):
                analysis_definition['ThemeArn'] = self._target_theme

            return 1
        
        except Exception as e:
            self._logger.error(f'An error occurred in migrate_analysis_handler function.\nError: {e}')
            return 0

    def create_dataset_handler(self, database_id: str) -> int:
        try:
            dataset_info = self._aws_datasets.describe_dataset(self._source_client, database_id)
            
            #Check if it's a full logic dataset
            if not dataset_info['PhysicalTableMap']:
                for id, logical_table_info in dataset_info['LogicalTableMap'].items():
                    # Intermediate Table is the table were the JOIN happens
                    if logical_table_info['Alias'] != 'Intermediate Table':
                        child_dataset_info = self._aws_datasets.describe_dataset(self._source_client, extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                        child_dataset_info = self._switch_datasource_arn(child_dataset_info)

                        if self._aws_datasets.create_dataset(self._target_client, child_dataset_info):
                            child_dataset_new_arn = logical_table_info['Source']['DataSetArn'].replace(self._source_region, self._target_region)
                            dataset_info['LogicalTableMap'][id]['Source']['DataSetArn'] = child_dataset_new_arn
            else:
                dataset_info = self._switch_datasource_arn(dataset_info)

            response = self._aws_datasets.create_dataset(self._target_client, dataset_info)
            
            if response == 2:
                return dataset_info['Arn'].replace(self._source_region, self._target_region)
            
            return response['Arn']
        except Exception as e:
            self._logger.error(f'An error occurred in create_dataset_handler function.\nError Message: {e}')
            return 0

    def create_template_handler(self, analysis_id: str, comment:str) -> int:
        try:
            # Describe analysis and gather required information
            analysis_info = self._aws_analysis.describe_analysis(self._source_client, analysis_id)
            dataset_references = self.create_dataset_references(analysis_info['DataSetArns'])

            # Attempt to create the template, return 2 if it fails
            if not self._aws_templates.create_template(self._target_client, analysis_info, comment, dataset_references):
                return 2

            self._logger.info('Template created successfully')
            return 1

        except Exception as e:
            self._logger.error(f'An error occurred in create_template_handler function.\nError: {e}')
            return 0
    
    def update_template_handler(self, analysis_id: str, comment: str) -> int:
        try:
            analysis_info = self._aws_analysis.describe_analysis(self._target_client, analysis_id)
            dataset_references = self._aws_datasets.create_dataset_references(self._target_client, analysis_info['DataSetArns'])

            self._aws_templates.update_template(self._target_client, analysis_info, comment, dataset_references)
            datasets_definition = [self._aws_datasets.describe_dataset(self._target_client, extract_id_from_arn(arn)) for arn in analysis_info['DataSetArns']]

            for dataset in datasets_definition:

                if 'LogicalTableMap' in dataset:
                    if dataset['LogicalTableMap'] and not dataset['PhysicalTableMap']:
                        logical_table = dataset['LogicalTableMap']
                        for id, logical_table_info in logical_table.items():
                            # Intermediate Table is the table were the JOIN happens
                            if logical_table_info['Alias'] != 'Intermediate Table':

                                datasets_definition.append({
                                    id:self._aws_datasets.describe_dataset(self._target_client, extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                                })
            return 1

        except Exception as e:
            self._logger.error(f'An error occurred in update_template_handler function.\nError: {e}')
            return 0

    def update_analysis_handler(self, analysis_id: str, version: str) -> int:
        try:
            analysis_info = self._aws_analysis.describe_analysis(analysis_id)
            template_info = self._aws_templates.describe_template(analysis_id, version)
            dataset_references = self.create_dataset_references(analysis_info['DataSetArns'])
            
            if self._aws_analysis.update_analysis(analysis_info, template_info, dataset_references) == 2:
                self._logger.info("Starting to recreate the analysis based on the template")
                self._aws_analysis.create_analysis(analysis_info, template_info, dataset_references)
                self._aws_analysis.grant_auth(analysis_id)
            
            return 1
        except Exception as e:
            self._logger.error(f'An error occurred in update_analysis_handler function.\nError Message: {e}')
            return 0
        