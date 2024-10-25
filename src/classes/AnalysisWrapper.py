import logging

class AWSAnalysis():

    def __init__(self, acc_id:str, user_arn:str, logger:logging):
        """Analysis Constructor
        Args:
            acc_id (str): aws account Id
            user_arn (str): AWS user ARN
            logger (object): logger object
        """
        self._analysis_id = None
        self._acc_id = acc_id
        self._user_arn = user_arn
        self._logger = logger

    def set_analysis_id(self, analysis_id: str):
        self._analysis_id = analysis_id

    def describe_analysis(self, client, analysis_id = None) -> dict:
        try:
            if analysis_id: self.set_analysis_id(analysis_id)

            analysis_descripion = client.describe_analysis(
                AwsAccountId=self._acc_id,
                AnalysisId=self._analysis_id
            )['Analysis']

            self._logger.debug(f'analysis_descripion {type(analysis_descripion)}: {analysis_descripion}')

            analysis_info = {
                'Arn': analysis_descripion['Arn'],
                'Id': self._analysis_id,
                'Name': analysis_descripion['Name'],
                'DataSetArns': analysis_descripion['DataSetArns'],
                'ThemeArn': analysis_descripion.get('ThemeArn', None)
            }

            return analysis_info
        
        except Exception as e:
            self._logger.error(f'An error ocurred in describe_analysis function.\n Error: {e}')

    def update_analysis(self, client, analysis_info:dict, template_info:dict, dataset_references:dict) -> int :
        """ Update the Analysis ID based in a template
        Args:
            analysis_id (str): Analysis Id
            template_info (dict): Template Dct
            dataset_references (dict): _description_
        Returns:
            int: Returns 1 if Succes, 0 if Failure and 2 if the Analysis alredy exists
        """
        try:
            client.update_analysis(
                AwsAccountId=self._acc_id,
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
                self._logger.error('Analysis is Not Found')
                return 2
            self._logger.error(f'An error ocurred in update_analysis function.\n Error: {e}')
            return 0
        else:
            self._logger.info('Analysis Updated Successfully')
            return 1

    def create_analysis(self, client, analysis_info:dict, template_info:dict, dataset_references:dict) -> int:
        """Create an Analysis in the given Region
        Args:
            analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
            template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
            dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function
        Returns:
            int: Returns 1 if Succes, 0 if Failure and 2 if the Analysis alredy exists
        """
        try:
            client.create_analysis(
                AwsAccountId=self._acc_id,
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
                self._logger.error('Analysis already exists')
                return 2
            elif str(type(e)) == "<class 'botocore.errorfactory.ResourceNotFoundException'>":
                self._logger.error('Analysis is Not Found')
                return 0
            else:
                self._logger.error(f'An error ocurred in create_analysis function.\n Error: {e}')
                return 0
        else:
            self._logger.info('Analysis Created With Success')
            return 1

    def list_analysis(self, client, next_token=None, analyses=[]):
        """Recursion to list all the analysis in a region"""
        try:
            if next_token:
                response = client.list_analyses(AwsAccountId=self.acc_id, NextToken=next_token)
            else:
                response = client.list_analyses(AwsAccountId=self.acc_id)

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
                return self.list_analysis(next_token, analyses)

        except Exception as e:
            self._logger.error(f"An error occurred in the list_analysis function.\nError: {e}")
            
        return analyses

    def list_deleted_analysis(self, client) -> list[dict[str]]:
        """Filter the Analysis whose status is equal to DELETED
        Returns:
            list[dict[str]]: Id, Name, Arn, Status 
        """
        try:    
            analysis_list = self.list_analysis(client, self._acc_id)
            data = [analysis for analysis in analysis_list if analysis['Status'] == 'DELETED']
            return data
        except Exception as e:
            self._logger.error(f"An error ocurred in list_deleted_analysis function.\n Error: {e}")

    def restore_analysis(self, client, analysis_id:str) -> int:
        """Restore a Deleted Analysis
        Args:
            analysis_id (str): deleted analysis id
        Returns:
            int: Returns 1 if Success and 0 if Fail  
        """
        try:
            restore_analysis = (client.restore_analysis(
                AwsAccountId=self._acc_id,
                AnalysisId=analysis_id
            ))
            self._logger.debug(f'restore_analysis {type(restore_analysis)}: {restore_analysis}')
            self._logger.info("Analysis Restoured")
            return 1
        except Exception as e:
            self._logger.error(f"An error ocurred in restore_analysis function.\nError: {e}")
            return 0

    def describe_analysis_definition(self, client, analysis_id:str = None) -> dict:
        """Retorna um Dicionário Descrevendo a Análise em Detalhes
        Args:
            analysis_id (str): analysis id
        Returns:
            dict:
        """
        try: 
            if analysis_id: self.set_analysis_id(analysis_id)

            self._logger.info("Describing analysis Definition")

            response = client.describe_analysis_definition(
                AwsAccountId=self._acc_id,
                AnalysisId= self._analysis_id
            )

            self._logger.debug(f'Analysis Definition\n{response}')
            
            data = {
                'Definition': response['Definition'],
                'Name': response['Name'],
                'Id': response['AnalysisId'],
                'ThemeArn': response.get('ThemeArn'),
            }

            self._logger.debug("#"*10,f"\nAnalysis Definition\n{data}")

            return data
        except Exception as e:
            self._logger.error(f"An error ocurred in describe_analysis_definition function.\nError: {e}")
            return 0
        
    def grant_auth(self, client, user_arn:str, analysis_id:str = None):
        ''' Atualiza as Permissões de Um Usuário Sobre uma Análise: Update, Restore, Delete, Query e Describe'''
        if analysis_id: self.set_analysis_id(analysis_id)
        try:
            client.update_analysis_permissions(
            AwsAccountId=self._acc_id,
            AnalysisId= self._analysis_id,
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
            self._logger.info("Permission Granted Successfully")
        except Exception as e:
            self._logger.error(f'An error ocurred in grant_auth function.\n Error: {e}')

    def create_analysis_by_definition(self, analysis_definition:dict) -> int:
        """Create an Analysis in the given Region

        Args:
            analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
            template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
            dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function
        """
        try:
            self.client.create_analysis(
                AwsAccountId = self._acc_id,
                AnalysisId = analysis_definition['Id'],
                Name = analysis_definition['Name'],
                Definition = analysis_definition['Definition']
            )
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
                self._logger.warning('Analysis Already Exists in this Region')
                return 2
            elif str(type(e)) == "<class 'botocorefactory.ResourceNotFoundException'>":
                self._logger.error('Analysis is Not Found')
                return 0
            else:
                self._logger.error(f'An error ocurred in create_analysis_by_definition function.\n Error: {e}')
                return 0
        else:
            self._logger.info('Analysis Created With Success')
            return 1