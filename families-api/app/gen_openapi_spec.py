import json
import os

from main import app

os.environ["GITHUB_SHA"] = "schema-generation"
os.environ["NAVIGATOR_DATABASE_URL"] = "schema-generation"
os.environ["CDN_URL"] = "schema-generation"


# After creating your FastAPI app
def gen_openapi_spec():
    openapi_spec = app.openapi()
    with open("families-api/openapi.json", "w") as f:
        json.dump(openapi_spec, f, indent=2)


# Call during build/startup
gen_openapi_spec()
