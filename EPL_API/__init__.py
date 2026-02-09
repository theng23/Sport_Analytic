# EPL_API/__init__.py
import azure.functions as func
import json
from services.epl_service import run_epl_pipeline


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        result = run_epl_pipeline()
        return func.HttpResponse(
            json.dumps(result, ensure_ascii=False, indent=2),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"ERROR: {str(e)}",
            status_code=500
        )
