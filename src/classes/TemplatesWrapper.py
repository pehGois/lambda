import logging

class AWSTemplates():

    def __init__(self, acc_id: str, user_arn:str, logger:logging):
        """Dataset Constructor
        Args:
            logger (object): logger object
            acc_id (str): aws account Id
            client (object): aws quicksight client
        """
        self.analysis_id = None
        self._acc_id = acc_id
        self._user_arn = user_arn
        self._logger = logger

    def set_dataset_id(self, dataset_id:str):
        self.analysis_id = dataset_id
    
    def create_template(self, client, analysis_info: dict[str,dict], comment:str, dataset_references) -> int:
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
                AwsAccountId=self._acc_id,
                TemplateId=analysis_info['Id'],
                Name=f'{analysis_info['Name']}_template',
                SourceEntity=SourceEntity,
                VersionDescription = comment
            )

        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
                self._logger.warning('Template Already Exists')
                return 0
            else:
                self._logger.error(f'An error ocurred in create_template function. Analysis Info - \n {analysis_info['Name']}\n   {analysis_info['Id']}.   \n    Error: {e}')
        else:
            self._logger.info(f'Template creation for {analysis_info['Name']} was SUCESSFULL \n Template ID: {analysis_info['Id']}')
            return 1

    def describe_template(self, client, template_id: str, version: str = None) -> dict:
        """Describes a template and returns a dictionary with keys: Arn, Id, Name, Version, Description."""
        try:
            template_params = {
                'AwsAccountId': self._acc_id,
                'TemplateId': template_id
            }
            if version: template_params['VersionNumber'] = int(version)
            
            template = client.describe_template(**template_params)['Template']

            return {
                'Arn': template.get('Arn'),
                'Id': template.get('TemplateId'),
                'Name': template.get('Name'),
                'Version': template.get('Version', {}).get('VersionNumber'),
                'Description': template.get('Version', {}).get('Description')
            }
        
        except Exception as e:
            self._logger.error(f'An error ocurred in describe_template function.\n Error: {e}')

    def delete_template(self, client, template_info:dict):
        ''' Função responsável por exluir um template '''
        try:
            client.delete_template(AwsAccountId=self._acc_id,
                TemplateId=template_info['Id'],
                VersionNumber=template_info['Version']
            )
        except Exception as e:
            self._logger.error(f'An error ocurred in delete_template function \n Error: {e}')

    def update_template(self, client, analysis_info:dict, comment:str, dataset_references:dict) -> int:
        try:
            if not comment: comment ='Nenhuma Observação'
            SourceEntity = {
                'SourceAnalysis': {
                    'Arn': analysis_info['Arn'],
                    'DataSetReferences': dataset_references
                }
            }
            client.update_template(
                AwsAccountId=self._acc_id,
                TemplateId=analysis_info['Id'],
                SourceEntity=SourceEntity,
                VersionDescription=comment,
                Name=f'{analysis_info['Name']}_template'
            )

        except Exception as e:
            self._logger.error(f'\nAn error ocurred in update_template function \n Error: {e}')
            return 0
        else:
            self._logger.info('Template Updated Sucefully')
            return 1
