from utils.utils import logger, extract_id_from_arn

class Handler():
    def __init__(self, AWSAnalysis, AWSDataset) -> None:
        self._aws_analysis = AWSAnalysis
        self._aws_datasets = AWSDataset

    def invoke(self, action, **kwargs):
        match action:
            case 'MIGRATION':
                return self.migrate_analysis_handler()
            case _:
                return 0
            
    def migrate_analysis_handler(self, analysis_id:str) -> int:
        """Handles the migration function and saves the .qs file into the S3."""
        try:
            analysis_definition = self._aws_analysis.describe_analysis_definition(analysis_id)
            arn_list_dict = analysis_definition['Definition']['DataSetIdentifierDeclarations']

            # Creating each dataset of the analysis in the new region
            for i, arn in enumerate(arn_list_dict):
                dataset_id = extract_id_from_arn(arn['DataSetArn'])
                new_arn = self.create_dataset_handler(dataset_id)
                analysis_definition['Definition']['DataSetIdentifierDeclarations'][i]['DataSetArn'] = new_arn

            # If there's a theme in the analysis, replace then by the new region theme
            if analysis_definition.get('ThemeArn'):
                analysis_definition['ThemeArn'] = self._aws_analysis.get_source_theme()

            return 1
        
        except Exception as e:
            logger.error(f'An error occurred in migrate_analysis_handler function.\nError: {e}')
            return 0
        
    def _switch_datasource_arn(self,dataset_info: dict) -> dict:
        """Switches the datasource Arn of the source analysis to the target one to enable migration."""

        if dataset_info['PhysicalTableMap']:
            PhysicalTableMap_id = next(iter(dataset_info['PhysicalTableMap']))
            dataset_info['PhysicalTableMap'][PhysicalTableMap_id]['CustomSql']['DataSourceArn'] = self._aws_datasets['arn']

        return dataset_info

    def create_dataset_handler(self,database_id: str) -> int:
        """Handles the dataset creation."""
        try:
            dataset_info = self._aws_datasets.describe_dataset(database_id)
            target_region = self._aws_datasets.get_target_region()
            source_region = self._aws_datasets.get_source_region()
            
            #Check if it's a full logic dataset
            if not dataset_info['PhysicalTableMap']:
                for id, logical_table_info in dataset_info['LogicalTableMap'].items():
                    # Intermediate Table is the table were the JOIN happens
                    if logical_table_info['Alias'] != 'Intermediate Table':
                        child_dataset_info = self._aws_datasets.describe_dataset(extract_id_from_arn(logical_table_info['Source']['DataSetArn']))
                        child_dataset_info = self._switch_datasource_arn(child_dataset_info)

                        if self._aws_datasets.create_dataset(child_dataset_info):
                            child_dataset_new_arn = logical_table_info['Source']['DataSetArn'].replace(source_region, target_region)
                            dataset_info['LogicalTableMap'][id]['Source']['DataSetArn'] = child_dataset_new_arn
            else:
                dataset_info = self._switch_datasource_arn(dataset_info)

            response = self._aws_datasets.create_dataset(dataset_info)
            
            if response == 2:
                return dataset_info['Arn'].replace(source_region, target_region)
            
            return response['Arn']
        except Exception as e:
            logger.error(f'An error occurred in create_dataset_handler function.\nError Message: {e}')
            return 0
        