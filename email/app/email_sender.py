import smtplib
import getpass
import os
from dotenv import load_dotenv
import logging

load_dotenv()

async def send_email_async(
    request:dict,
):
    try:
        our_email = os.getenv("SENDER_EMAIL", "habitbuilderfeedback@gmail.com")
        target_email = request.get("email")
        if not target_email:
            raise Exception("Email was not given")

        email_topic = "Price notification"
        email_text = f"The price on the unit {request.get("name", "")} has reached a threshold limit"

        password = os.getenv("SENDER_PASSWORD")
        if not password:
            raise Exception("Error while getting password from .env")

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()

        server.login(our_email, password)
        subject = email_topic
        body = email_text
        message = f'Subject: {subject}\n\n{body}'

        server.sendmail(our_email, target_email, message)
        server.quit()
        return message
    except Exception as e:
        logging.error("An error occurred while sending the email:", e)
        return None
