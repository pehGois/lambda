
import json
#from mangum import Mangum
from typing import Optional
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Form, Request

def fast_api(app:FastAPI, actions:list, lambda_handler):
    app.mount("/static", StaticFiles(directory="./src/static"), name="static")
    
    #handler = Mangum(app)
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
                {"request":request, "ACTIONS": actions}
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
        import_mode: Optional[str] = Form(None),
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
                "import_mode": import_mode
            }
            return templates.TemplateResponse(
                'response.html', 
                {"request": request, "response": json.dumps(lambda_handler(event, None), indent=4, ensure_ascii=False)}
            )
        except Exception as e:
            return templates.TemplateResponse('error.html', {"request":request,"error":e,"error_type":type(e).__name__})