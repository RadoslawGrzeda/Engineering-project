from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from db_connection import get_db
from sqlalchemy.orm import Session
from fastapi import Depends
from db_connection import engine
import model
import rout

app = FastAPI()
app.title = "CRM"
app.description = "Customer Dead Letter Management API"
app.include_router(rout.router)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
