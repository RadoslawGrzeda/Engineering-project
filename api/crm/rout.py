import os
import model, schema
from db_connection import get_db
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends, HTTPException
from kafka_producer import get_kafka_producer

router = APIRouter(
    prefix="/crm",
    tags=["CRM"],
)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crm_client")


@router.get("/dead_letters", response_model=list[schema.DeadLetterBase])
def get_dead_letters(db: Session = Depends(get_db)):
    return db.query(model.DeadLetter).all()


@router.get("/dead_letters/{dead_letter_id}", response_model=schema.DeadLetterBase)
def get_dead_letter(dead_letter_id: int, db: Session = Depends(get_db)):
    dl = db.query(model.DeadLetter).filter(model.DeadLetter.id == dead_letter_id).first()
    if not dl:
        raise HTTPException(status_code=404, detail="Dead letter not found")
    return dl


@router.put("/dead_letters/{dead_letter_id}", response_model=schema.DeadLetterBase)
def update_dead_letter(dead_letter_id: int, payload: schema.DeadLetterUpdate, db: Session = Depends(get_db)):
    dl = db.query(model.DeadLetter).filter(model.DeadLetter.id == dead_letter_id).first()
    if not dl:
        raise HTTPException(status_code=404, detail="Dead letter not found")
    dl.raw_payload = payload.raw_payload
    db.commit()
    db.refresh(dl)
    return dl


@router.post("/dead_letters/{dead_letter_id}/resubmit", response_model=schema.DeadLetterBase)
def resubmit_dead_letter(dead_letter_id: int, db: Session = Depends(get_db)):
    dl = db.query(model.DeadLetter).filter(model.DeadLetter.id == dead_letter_id).first()
    if not dl:
        raise HTTPException(status_code=404, detail="Dead letter not found")

    producer = get_kafka_producer()
    producer.send(KAFKA_TOPIC, value=dl.raw_payload)
    producer.flush()

    dl.status = "RESUBMITTED"
    dl.retry_count = (dl.retry_count or 0) + 1
    db.commit()
    db.refresh(dl)
    return dl


@router.post("/addUser", status_code=202)
def add_user(payload: schema.AddUserRequest):
    producer = get_kafka_producer()
    message = payload.model_dump(mode="json")
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    return {"status": "accepted", "person_id": payload.account.person_id}