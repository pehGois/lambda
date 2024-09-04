from lambda_function import lambda_handler
event = {
    'email': 'pedro.nunes@omotor.com.br',
    'analysis_id': '88a05651-36e7-48b6-bcef-2977b02a6058',
    'action': 'ANALYSIS_UPDATE',
    'target_region': 'us-east-1',
    'source_region': 'us-east-1',
    'version': 1, 
    'comment': "Testando o Lambda",
    'bucket' : 'teste-ml-omotor',
    'stakeholder': 'omotor'
}
lambda_handler(event, None)