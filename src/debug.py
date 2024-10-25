from lambda_function import lambda_handler
event = {
    'email': 'pedro.nunes@omotor.com.br',
    'analysis_id': '88a05651-36e7-48b6-bcef-2977b02a6058',# 4cd18184-821d-4e5d-b757-9ecd02988195
    'action': 'MIGRATION',
    'target_region': 'us-east-1',
    'source_region': 'us-west-2',
    'version': 1, 
    'comment': "Testando o Lambda",
    'bucket' : 'teste-ml-omotor',
    'stakeholder': 'OMOTOR'
}
print(lambda_handler(event, None))