import secrets

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import ssl
from src.api.v1.routers import authentication
from fastapi.middleware.cors import CORSMiddleware
from fastapi import security

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)

ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
try:
    ssl_context.load_cert_chain('/etc/letsencrypt/live/amsservice.hypin.in/fullchain.pem',
                                keyfile='/etc/letsencrypt/live/amsservice.hypin.in/privkey.pem')
except FileNotFoundError:
    pass

security = HTTPBasic()


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, "createcDev")
    correct_password = secrets.compare_digest(credentials.password, "kwf5aAjJpxE7")
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


app.include_router(authentication.router)


@app.get("/")
async def root():
    return Response(status_code=status.HTTP_418_IM_A_TEAPOT, media_type='text/plain')


@app.get("/health")
async def health():
    return {"status": "ok",
            "message": "At least the server works..."}


@app.get("/docs")
async def get_documentation(username: str = Depends(get_current_username)):
    return get_swagger_ui_html(openapi_url="/openapi.json", title="docs")


@app.get("/openapi.json")
async def openapi(username: str = Depends(get_current_username)):
    return get_openapi(title="FastAPI", version="0.1.0", routes=app.routes)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
