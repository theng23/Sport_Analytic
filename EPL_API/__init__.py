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


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        print("START EPL PIPELINE")
        result = run_epl_pipeline()
        print("PIPELINE DONE")
        return func.HttpResponse(json.dumps(result), mimetype="application/json")
    except Exception as e:
        print("ERROR:", str(e))
        raise
