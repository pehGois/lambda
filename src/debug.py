from lambda_function import lambda_handler
event = {
    'email': 'pedro.nunes@omotor.com.br',
    'analysis_id': '4cd18184-821d-4e5d-b757-9ecd02988195',
    'action': 'MIGRATION',
    'target_region': 'us-west-2',
    'source_region': 'us-east-1',
    'version': 1, 
    'comment': "Testando o Lambda",
    'bucket' : 'teste-ml-omotor',
    'stakeholder': 'omotor'
}
lambda_handler(event, None)