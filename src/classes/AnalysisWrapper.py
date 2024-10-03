from AWSClient import AWSClient
class AWSAnalysis(AWSClient):

    def __init__(self, resource:str, region: object, acc_id:str, logger: object = None) -> None:
        """Analysis Constructor
        Args:
            logger (object): logger object
            acc_id (str): aws account Id
            client (object): aws quicksight client
        """
        self.__analysis_id = None
        super(AWSAnalysis, self).__init__(resource, region, acc_id, logger)

    def set_analysis_id(self, analysis_id: str):
        self.__analysis_id = analysis_id

    def describe_analysis(self) -> dict :
        """Describes the analysis details
        Args:
            analysis_id (str): Analysis Id
        Returns:
            dict
        """
        try:
            analysis_descripion = self.client.describe_analysis(AwsAccountId=self.acc_id,AnalysisId=self.__analysis_id)['Analysis']

            analysisInfo = {
                'Arn': analysis_descripion['Arn'],
                'Id': self.__analysis_id,
                'Name': analysis_descripion['Name'],
                'DataSetArns': analysis_descripion['DataSetArns']
            }

            if 'ThemeArn' in analysis_descripion:
                analysisInfo['ThemeArn'] = analysis_descripion['ThemeArn']
            else: 
                analysisInfo['ThemeArn'] = None

            return analysisInfo
        except Exception as e:
            self._logger.error(f'An error ocurred in describe_analysis function.\n Error: {e}')

    def update_analysis(self, analysis_info:dict, template_info:dict, dataset_references:dict) -> int :
        """ Update the Analysis ID based in a template
        Args:
            analysis_id (str): Analysis Id
            template_info (dict): Template Dct
            dataset_references (dict): _description_
        Returns:
            int: Returns 1 if Succes, 0 if Failure and 2 if the Analysis alredy exists
        """
        try:
            self.client.update_analysis(
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

    def create_analysis(self, analysis_info:dict, template_info:dict, dataset_references:dict) -> int:
        """Create an Analysis in the given Region
        Args:
            analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
            template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
            dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function
        Returns:
            int: Returns 1 if Succes, 0 if Failure and 2 if the Analysis alredy exists
        """
        try:
            self.client.create_analysis(
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

    def list_analysis(self, next_token=None, analyses=[]):
        """Recursion to list all the analysis in a region"""
        try:
            if next_token:
                response = self._client.list_analyses(AwsAccountId=self.acc_id, NextToken=next_token)
            else:
                response = self._client.list_analyses(AwsAccountId=self.acc_id)

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

    def list_deleted_analysis(self) -> list[dict[str]]:
        """Filter the Analysis whose status is equal to DELETED
        Returns:
            list[dict[str]]: Id, Name, Arn, Status 
        """
        try:    
            analysis_list = self.list_analysis(self.client, self.acc_id)
            data = [analysis for analysis in analysis_list if analysis['Status'] == 'DELETED']
            return data
        except Exception as e:
            self._logger.error(f"An error ocurred in list_deleted_analysis function.\n Error: {e}")

    def restore_analysis(self, analysis_id:str) -> int:
        """Restore a Deleted Analysis
        Args:
            analysis_id (str): deleted analysis id
        Returns:
            int: Returns 1 if Success and 0 if Fail  
        """
        try:
            self.self.logger.debug(self.client.restore_analysis(
                AwsAccountId=self.acc_id,
                AnalysisId=analysis_id
            ))
            self.self.logger.info("Analysis Restoured")
            return 1
        except Exception as e:
            self.self.logger.error(f"An error ocurred in restore_analysis function.\nError: {e}")
            return 0

    def describe_analysis_definition(self, analysis_id:str) -> dict:
        """Retorna um Dicionário Descrevendo a Análise em Detalhes
        Args:
            analysis_id (str): analysis id
        Returns:
            dict:
        """
        try: 
            self.self.logger.info("Describing analysis Definition")
            response = self.client.describe_analysis_definition(
                AwsAccountId=self.acc_id,
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
            self.self.logger.error(f"An error ocurred in describe_analysis_definition function.\nError: {e}")
            return 0
        
    def grant_auth(self, user_arn:str):
        ''' Atualiza as Permissões de Um Usuário Sobre uma Análise: Update, Restore, Delete, Query e Describe'''
        try:
            self.client.update_analysis_permissions(
            AwsAccountId=self.acc_id,
            AnalysisId= self.__analysis_id,
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
            self.self.logger.info("Permission Granted Successfully")
        except Exception as e:
            self.self.logger.error(f'An error ocurred in grant_auth function.\n Error: {e}')

    def create_analysis_by_definition(self, analysis_definition:dict) -> int:
        """Create an Analysis in the given Region

        Args:
            analysis_info (dict): analysis info dictionary, can be obtained by Describe Analysis Function
            template_info (dict): Tempate Info dictionary, can be obtained by Desribe Template Function
            dataset_references (dict): Dataset References, can be obtained by Create Data Reference Function

        Returns:
            int: 1 if Success, 2 If already Exists, 1 if Fail
        """
        try:
            self.client.create_analysis(
                AwsAccountId = self.acc_id,
                AnalysisId = analysis_definition['Id'],
                Name = analysis_definition['Name'],
                Definition = analysis_definition['Definition']
            )
        except Exception as e:
            if str(type(e)) == "<class 'botocore.errorfactory.ResourceExistsException'>":
                self.logger.warning('Analysis Already Exists in this Region')
                return 2
            elif str(type(e)) == "<class 'botocorefactory.ResourceNotFoundException'>":
                self.logger.error('Analysis is Not Found')
                return 0
            else:
                self.logger.error(f'An error ocurred in create_analysis_by_definition function.\n Error: {e}')
                return 0
        else:
            self.logger.info('Analysis Created With Success')
            return 1