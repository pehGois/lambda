import os
import boto3
from classes.AnalysisWrapper import AWSAnalysis
from classes.DatasetWrapper import AWSDataset
from classes.TemplatesWrapper import AWSTemplates
from classes.Handler import Handler
from utils.helper import *
from fast_api import *

from dotenv import load_dotenv
load_dotenv("src/.env/.env")

AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID')
THEME_ARN_PROD = os.environ.get('THEME_ARN_PROD')
THEME_ARN_DEV = os.environ.get('THEME_ARN_DEV')
PROD_REGION = os.environ.get('PROD_REGION')
DEV_REGION = os.environ.get('DEV_REGION')
PROD_ARN = os.environ.get('PROD_ARN')
DEV_ARN = os.environ.get('DEV_ARN')
BUCKET = os.environ.get('BUCKET')

ACTIONS = ["LIST_DELETED_ANALYSIS","TEMPLATE_CREATION","ANALYSIS_UPDATE","TEMPLATE_UPDATE","MIGRATION","RESTORE_ANALYSIS"]
logger = get_logger()

def lambda_handler(event: dict[str,str], context):
    try:
        
        bucket = BUCKET
        email = event['email']
        action = event['action']
        source = event['source_region']
        target = event['target_region']
        version = event.get('version')
        analysis_id = event.get('analysis_id')
        import_mode = event.get('import_mode')
        stakeholder = event.get('stakeholder', 'OMOTOR')
        comment = event.get('comment', 'Nenhum Comentário')

        prod_client = boto3.client('quicksight', region_name=PROD_REGION)
        user_arn = search_user(prod_client, AWS_ACCOUNT_ID, email)
        # s3_client = boto3.client('s3')

        client_map = {
            DEV_REGION: {"data_source_arn": DEV_ARN, "theme": THEME_ARN_DEV, "region-id": DEV_REGION},
            PROD_REGION: {"data_source_arn": PROD_ARN, "theme": THEME_ARN_PROD, 'region-id': PROD_REGION}
        }

        if action not in ACTIONS:
            logger.critical(f"Invalid action: {action}. Available actions: {', '.join(ACTIONS)}")
            return return_message(action, 1, response = "Invalid action: {action}", user = email)
        
        source_info = client_map[source]
        target_info = client_map[target]
        
        handler = Handler(
            source_info, 
            target_info,
            AWSAnalysis(AWS_ACCOUNT_ID, user_arn, logger),
            AWSDataset(AWS_ACCOUNT_ID, user_arn, logger),
            AWSTemplates(AWS_ACCOUNT_ID, user_arn, logger),
            logger
        )
        status = handler.invoke(action, analysis_id = analysis_id, comment = comment, version = version, stakeholder = stakeholder, import_mode = import_mode, bucket = bucket)

        return return_message(action, status, user = email, analysis_id = analysis_id,
                              comment = comment, source_region = source_info['region-id'],
                                target_info = target_info['region-id'], stakeholder = stakeholder)

    except Exception as e:
        logger.error(f"Error occurred: {e}\nType: {type(e)}", exc_info=True)

app = FastAPI()
fast_api(app, ACTIONS, lambda_handler)