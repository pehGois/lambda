import boto3
from handlers import *
import datetime
import os
from mangum import Mangum
from fastapi import FastAPI, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

PROD_ARN = os.environ.get('PROD_ARN')
DEV_ARN = os.environ.get('DEV_ARN')
AWS_ACCOUNT_ID = os.environ.get('AWS_ACCOUNT_ID')
PROD_REGION = os.environ.get('PROD_REGION')
DEV_REGION = os.environ.get('DEV_REGION')
BUCKET = os.environ.get('BUCKET')
THEME_ARN_DEV = os.environ.get('THEME_ARN_DEV')
THEME_ARN_PROD = os.environ.get('THEME_ARN_PROD')
ACTIONS = {
            "TEMPLATE_CREATION": create_template_handler,
            "TEMPLATE_UPDATE": update_template_handler,
            "ANALYSIS_UPDATE": update_analysis_handler,
            "RESTORE_ANALYSIS": restore_analysis,
            "LIST_DELETED_ANALYSIS": list_deleted_analysis,
            "MIGRATION": migrate_analysis_handler
        }
with open('style.css', "r") as style: style = style.read()

app = FastAPI()
handler = Mangum(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", response_class=HTMLResponse)
async def get_form():
    try:
        return f"""
<html lang="pt-br">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Quicksight API</title>
    <style>{style}</style>
</head>
<body>
    <main class="form-container">
        <form id="request_form" action="/submit" method="post">
            <label for="email">Email:</label>
            <input type="email" id="email" name="email" required>

            <label for="analysis_id">Analysis ID: (Opcional)</label>
            <input type="text" id="analysis_id" name="analysis_id">

            <label for="stakeholder">Stakeholder: (Opcional)</label>
            <input type="text" id="stakeholder" name="stakeholder">

            <label for="action">Action:</label>
            <select id="action" name="action" required>
                {[f'<option value={action_name}>{action_name}</option>' for action_name, function in ACTIONS.items()]}
            </select>

            <label for="source_region">Source Region:</label>
            <select id="source_region" name="source_region" required>
                <option value="us-west-2">us-west-2</option>
                <option value="us-east-1">us-east-1</option>
            </select>

            <label for="target_region">Target Region: (Opcional)</label>
            <select id="target_region" name="target_region">
                <option value="us-east-1">us-east-1</option>
                <option value="us-west-2">us-west-2</option>
            </select>

            <label for="version">Version (Opcional):</label>
            <input type="number" id="version" name="version">

            <label for="comment">Comment (Opcional):</label>
            <textarea id="comment" name="comment" rows="4" cols="50"></textarea>

            <input type="submit" value="Enviar">
        </form>        
    </main>
</script>
</body>
</html>
"""
    except Exception as e:
        return {'Erro': type(e), "Mensagem de Erro": {e}}

@app.post("/submit", response_class=HTMLResponse)
async def submit_form(
    email: str = Form(...),
    analysis_id: Optional[str] = Form(None),
    stakeholder: Optional[str] = Form(None),
    action: str = Form(...),
    source_region: str = Form(...),
    target_region: str = Form(...),
    version: Optional[str] = Form(None),
    comment: Optional[str] = Form(None)
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
        return f""" 
        <html>
            <head>
                <title>Response</title>
            </head>
            <style>{style}</style>
            <body>
                <main>
                    <h2>Requisição Realizada</h2>
                    <p>Resposta da API Quicksight</p>
                    <pre>{json.dumps(lambda_handler(event, None), indent=4, ensure_ascii=False)}</pre>
                    <a href="/">Voltar ao Formulário</a>
                </main>
            </body>
        </html> 
        """
    except Exception as e:
        return return_json_message({'Erro': type(e), "Mensagem de Erro": {e}}, email)

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

        if action not in ACTIONS:
            logger.critical(f"Invalid action: {action}.")
            return return_json_message(f"Invalid action: {action}", email)

        logger.info(f"Starting {action.replace('_', ' ').title()}")
        
        if action == "LIST_DELETED_ANALYSIS":
            del_analysis_list = ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID)
            return del_analysis_list if del_analysis_list else return_log_message(action, email, source)
        
        elif action == "MIGRATION":
            result = ACTIONS[action](AWS_ACCOUNT_ID, analysis_id, user_arn, source_client, target_client, s3_client, bucket, stakeholder)

        else:
            result = ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID, analysis_id, comment, email, s3_client, bucket, stakeholder) if "TEMPLATE" in action else ACTIONS[action](source_client['client'], AWS_ACCOUNT_ID, analysis_id, version, user_arn)

        return return_log_message(action, email, source, target, result, analysis_id, comment)

    except Exception as e:
        logger.error(f"Error occurred: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail={'Error': type(e).__name__, "Error Message": str(e)})