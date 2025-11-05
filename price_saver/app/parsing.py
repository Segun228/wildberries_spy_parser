import smtplib
import getpass
import os
from dotenv import load_dotenv
import logging

load_dotenv()

async def handle_parsing_request(
    request:dict,
):
    try:
        pass
        #TODO
    except Exception as e:
        logging.error("An error occurred while sending the email:", e)
        return None