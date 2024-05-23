import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import pandas as pd

EMAIL_FROM = 'tempmailer0801@gmail.com'
PASSWORD = 'racpjhggrsfjkyje'

recipient_list = ['hinal@ibisp.in', 'tempmailer0801@gmail.com']
EMAIL_TO = ', '.join(recipient_list)

def attach_file_to_email(email_message, filename, extra_headers=None):
    print("FILENAME: ", filename)
    with open(filename, "rb") as f:
        file_attachment = MIMEApplication(f.read())
    file_attachment.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )
    if extra_headers is not None:
        for name, value in extra_headers.items():
            file_attachment.add_header(name, value)
    email_message.attach(file_attachment)

def send_email(html_content, image_path):
    from datetime import datetime
    date_str = str(datetime.now().strftime("%d-%m-%Y"))

    email_message = MIMEMultipart()
    email_message['From'] = EMAIL_FROM
    email_message['To'] = EMAIL_TO
    email_message['Subject'] = f'Weekly Percentage Change in Stocks Report - {date_str}'

    email_message.attach(MIMEText(html_content, "html"))

    attach_file_to_email(email_message, image_path, {'Content-ID': '<myimageid>'})

    email_string = email_message.as_string()

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(EMAIL_FROM, PASSWORD)
        server.sendmail(EMAIL_FROM, recipient_list, email_string)