from fastapi import FastAPI, Form, HTTPException, APIRouter
from pydantic import EmailStr
import random
import os
import aiosmtplib
from email.message import EmailMessage
from datetime import datetime, timedelta

router = APIRouter()

# AWS SES SMTP Settings
SMTP_HOST = "email-smtp.us-east-1.amazonaws.com"
SMTP_PORT = 587
SMTP_USER = os.getenv("SES_SMTP_USERNAME")
SMTP_PASS = os.getenv("SES_SMTP_PASSWORD")
FROM_EMAIL = "info@createc.in"

# In-memory store: { email: { otp: str, expires_at: datetime } }
otp_store = {}

# Generate a 6-digit OTP
def generate_otp():
    return str(random.randint(100000, 999999))

# Endpoint to send OTP
@router.post("/v1/otp/email")
async def send_otp_email(email: EmailStr = Form(...)):
    otp = generate_otp()
    expires_at = datetime.utcnow() + timedelta(minutes=5)  # OTP valid for 5 mins

    # Save OTP
    otp_store[email] = {"otp": otp, "expires_at": expires_at}

    # Send email
    message = EmailMessage()
    message["From"] = FROM_EMAIL
    message["To"] = email
    message["Subject"] = "Your OTP Code"
    message.set_content(f"Your OTP code is: {otp}\nIt is valid for 5 minutes.")

    try:
        await aiosmtplib.send(
            message,
            hostname=SMTP_HOST,
            port=SMTP_PORT,
            username=SMTP_USER,
            password=SMTP_PASS,
            start_tls=True
        )
        return {"message": "OTP sent successfully", "email": email}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send email: {e}")


# Endpoint to validate OTP
@router.post("/v1//otp/validate")
async def validate_otp(email: EmailStr = Form(...), otp: str = Form(...)):
    record = otp_store.get(email)

    if not record:
        raise HTTPException(status_code=404, detail="No OTP found for this email")

    if datetime.utcnow() > record["expires_at"]:
        del otp_store[email]
        raise HTTPException(status_code=400, detail="OTP has expired")

    if record["otp"] != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    # Success: optionally delete the OTP
    del otp_store[email]
    return {"message": "OTP validated successfully", "email": email}