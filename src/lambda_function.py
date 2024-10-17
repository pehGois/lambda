import os
import boto3
from mangum import Mangum
from typing import Optional
from utils.handlers import *
from fastapi.responses import HTMLResponse
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates

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

ACTIONS = {
    'TESTE': "Oi",
    "LIST_DELETED_ANALYSIS": list_deleted_analysis,
    "TEMPLATE_CREATION": create_template_handler,
    "ANALYSIS_UPDATE": update_analysis_handler,
    "TEMPLATE_UPDATE": update_template_handler,
    "MIGRATION": migrate_analysis_handler,
    "RESTORE_ANALYSIS": restore_analysis,
}

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
handler = Mangum(app)
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def show_form(request:Request):
    try:
        return templates.TemplateResponse(
            'form.html', 
            {"request":request, "ACTIONS": ACTIONS}
        )
    except Exception as e:
        return templates.TemplateResponse('error.html', {"request":request,"error":e,"error_type":type(e).__name__})

@app.post("/submit", response_class=HTMLResponse)
async def submit_form(
    request: Request,
    email: str = Form(...), 
    analysis_id: Optional[str] = Form(None),
    stakeholder: Optional[str] = Form(None),
    action: str = Form(...), 
    source_region: str = Form(...),
    target_region: str = Form(...),
    version: Optional[str] = Form(None),
    comment: Optional[str] = Form(None),
):
    try:
        event = {
            "email": email,
            "analysis_id": analysis_id,
            "stakeholder": stakeholder,
            "action": action,
            "target_region": target_region,
            "source_region": source_region,
            "version": version,
            "comment": comment,
        }
        return templates.TemplateResponse(
            'response.html', 
            {"request": request, "response": json.dumps(lambda_handler(event, None), indent=4, ensure_ascii=False)}
        )
    except Exception as e:
        return templates.TemplateResponse('error.html', {"request":request,"error":e,"error_type":type(e).__name__})

def lambda_handler(event: dict[str,str], context):
    """Function that handles all the lambda api

    Args:
        event (dict): dictionary containing all the variables
        context (dict): context of execution in lamda

    Returns:
        dict: Dict with response field
    """
    try:
        required_params = ['email', 'source_region', 'action']
        for param in required_params:
            if not event.get(param):
                logger.critical(f"Missing required event parameter: {param}.")
                return return_json_message(f"Missing required event parameters: {', '.join(required_params)}", event.get('email',""))

        action = event['action'].upper()
        source = event['source_region']
        target = event['target_region']
        email = event['email']
        analysis_id = event.get('analysis_id')
        version = event.get('version')
        comment = event.get('comment')
        stakeholder = event.get('stakeholder')
        bucket = BUCKET

        prod_client = boto3.client('quicksight', region_name=PROD_REGION)
        dev_client = boto3.client('quicksight', region_name=DEV_REGION)
        s3_client = boto3.client('s3')

        client_map = {
            DEV_REGION: {"client": dev_client, "arn": DEV_ARN, "theme": THEME_ARN_DEV, "region": DEV_REGION},
            PROD_REGION: {"client": prod_client, "arn": PROD_ARN, "theme": THEME_ARN_PROD, 'region': PROD_REGION}
        }

        source_client = client_map[source]
        target_client = client_map[target]
        user_arn = search_user(prod_client, AWS_ACCOUNT_ID, email)
        print(action)
        if action not in ACTIONS:
            logger.critical(f"Invalid action: {action}.")
            return return_json_message(f"Invalid action: {action}", email)

        logger.info(f"Starting {action.replace('_', ' ').title()}")
        
        if action == "LIST_DELETED_ANALYSIS":
            del_analysis_list = ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID)
            return del_analysis_list if del_analysis_list else return_log_message(action, email, source)
        
        elif action == "MIGRATION":
            result = ACTIONS[action](AWS_ACCOUNT_ID, analysis_id, user_arn, source_client, target_client, s3_client, bucket, stakeholder)

        elif action == 'TESTE':
            result = ACTIONS.get(action)
        else:
            result = ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID, analysis_id, comment, email, s3_client, bucket, stakeholder) if "TEMPLATE" in action else ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID, analysis_id, version, user_arn)

        return return_log_message(action, email, source, target, result, analysis_id, comment)

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail={'Error': type(e).__name__, "Error Message": str(e)})