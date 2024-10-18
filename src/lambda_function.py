import os
import boto3
from classes.AnalysisWrapper import AWSAnalysis
from classes.DatasetWrapper import AWSDataset
from classes.Handler import Handler
from utils.handlers import *
from fast_api import *

from dotenv import load_dotenv
load_dotenv("env/.env")

AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID')
THEME_ARN_PROD = os.environ.get('THEME_ARN_PROD')
THEME_ARN_DEV = os.environ.get('THEME_ARN_DEV')
PROD_REGION = os.environ.get('PROD_REGION')
DEV_REGION = os.environ.get('DEV_REGION')
PROD_ARN = os.environ.get('PROD_ARN')
DEV_ARN = os.environ.get('DEV_ARN')
BUCKET = os.environ.get('BUCKET')

ACTIONS = ["LIST_DELETED_ANALYSIS","TEMPLATE_CREATION","ANALYSIS_UPDATE","TEMPLATE_UPDATE","MIGRATION","RESTORE_ANALYSIS",]

def lambda_handler(event: dict[str,str], context):
    """  Function that handles all the lambda api
    Args:
        event (dict): dictionary containing all the variables
        context (dict): context of execution in lamda
    Returns:
        dict: Dict with response field
    """
    try:
        
        action = event['action'].upper()
        source = event['source_region']
        target = event['target_region']
        email = event['email']
        analysis_id = event.get('analysis_id')
        version = event.get('version')
        comment = event.get('comment')
        stakeholder = event.get('stakeholder')
        import_mode = event.get('import_mode')
        bucket = BUCKET

        prod_client = boto3.client('quicksight', region_name=PROD_REGION)
        s3_client = boto3.client('s3')

        client_map = {
            DEV_REGION: {"data_source_arn": DEV_ARN, "theme": THEME_ARN_DEV, "region-id": DEV_REGION},
            PROD_REGION: {"data_source_arn": PROD_ARN, "theme": THEME_ARN_PROD, 'region-id': PROD_REGION}
        }

        if action not in ACTIONS:
            logger.critical(f"Invalid action: {action}.")
            return return_json_message(f"Invalid action: {action}", email)
        
        source_info = client_map[source]
        target_info = client_map[target]
        
        user_arn = search_user(prod_client, AWS_ACCOUNT_ID, email)
        handler = Handler(AWSAnalysis(source_info,target_info,AWS_ACCOUNT_ID,user_arn),AWSDataset(source,target,AWS_ACCOUNT_ID,user_arn))
        handler.invoke(action, analysis_id = analysis_id, comment = comment, version = version, stakeholder = stakeholder, import_mode = import_mode, bucket = bucket)

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)

app = FastAPI()
fast_api(app, ACTIONS, lambda_handler)