import time
from tendo import singleton
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
import pandas as pd
import logging
from pymongo import MongoClient
import boto3
import requests
from sendgrid.helpers.mail import Mail
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
)

postgres_host = os.getenv("PG_HOST")
postgres_db = os.getenv("PG_DATABASE")
postgres_user = os.getenv("PG_USER")
postgres_password = os.getenv("PG_PASSWORD")
postgres_port = os.getenv("PG_PORT")

aws_access_key_id = os.getenv('AWS_ACCESS')
aws_secret_access_key = os.getenv('AWS_SECRET')
bucket_name = os.getenv('DEST_AWS_BUCKET_NAME')

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')

MONGO_URL = os.getenv('MONGO_URL')
client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

pgconn = psycopg2.connect(
    host=postgres_host,
    database=postgres_db,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password
)

LIMIT = 1

currtime = int(time.time())
bucket_time = int(currtime / (90 * 24 * 60 * 60))


def sendMailToClient(to_emails, template_id, dynamic_template_data):
    logging.info("sendMailToClient called...")
    api_key = SENDGRID_API_KEY
    url = 'https://api.sendgrid.com/v3/mail/send'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {api_key}',
    }

    for to_email in to_emails:
        message = Mail(
            from_email='info@finkraft.ai',
            to_emails=to_email,
        )
        message.template_id = template_id
        message.dynamic_template_data = dynamic_template_data

        try:
            # Convert the Mail object to JSON
            response = requests.post(
                url,
                headers=headers,
                json=message.get(),
                verify=False  # Disable SSL verification
            )
            logging.info(f"Email sent to {to_email} successfully! Status code: {response.status_code}")
        except Exception as e:
            logging.info(f"Error sending email to {to_email}: {e}")


def uploadFile(filename):
    try:
        logging.info("uploadFile called...")
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
        object = f"{bucket_time}/{filename}"
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{object}"
        s3.upload_file(filename,
                       bucket_name,
                       Key=object
                       )
        if os.path.exists(filename):
            os.remove(filename)
        return s3_url
    except Exception as e:
        logging.info("Exception happened in the getS3Url: " + str(e))
        if os.path.exists(filename):
            os.remove(filename)
        return None

def getData(wsname):
    logging.info("getData called...")
    filename = '_'.join(wsname) + "_" + str(currtime) + ".xlsx"
    total_records = 0

    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for i in range(len(wsname)):
            with pgconn.cursor() as cursor:
                select_query = "SELECT * FROM mmt_flight_recon WHERE \"Customer_Name\" ILIKE %s"
                logging.info("Query: " + str(select_query))
                logging.info("Param: " + str(wsname[i]))
                cursor.execute(select_query, (wsname[i],))
                results = cursor.fetchall()
                logging.info("No. of record: " + str(len(results)))
                column_names = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(results, columns=column_names)

            # Convert columns to numeric datatype where possible
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except ValueError:
                    pass  # If conversion fails, keep the original data

            # Split data into chunks of 50,000 rows
            for chunk_num, chunk in enumerate(range(0, len(df), 50000)):
                sheet_name = f"{wsname[i]}_{chunk_num + 1}"
                df_chunk = df.iloc[chunk:chunk + 50000]
                df_chunk.to_excel(writer, sheet_name=sheet_name, index=False)

                # Open the workbook to format the header
                wb = writer.book
                ws = wb[sheet_name]

                # Set header style (font size 12, yellow fill)
                for cell in ws[1]:
                    cell.font = Font(size=12, bold=True)
                    cell.fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

                total_records += len(df_chunk)

    logging.info(f"Total records written: {total_records}")
    return filename, total_records

def getWorkspaceName(workspaceids):
    logging.info("getWorkspaceName called...")
    finalresult = []
    workspaceids = tuple(workspaceids)
    with pgconn.cursor() as cursor:
        select_query = "SELECT name FROM workspaces WHERE id in %s"
        cursor.execute(select_query, (workspaceids,))
        results = cursor.fetchall()
        for row in results:
            finalresult.append(row[0])
        return finalresult


def getPendingJob():
    logging.info("getPendingJob called...")
    db = client['gstservice']
    collection = db['recon_report']
    # result = list(collection.find({"status": "PENDING"}).limit(LIMIT))
    result = list(collection.find({"status": "PENDING"}).sort({"createdBy": -1}).limit(LIMIT))
    return result


if __name__ == '__main__':
    try:
        me = singleton.SingleInstance()
        logging.info("======================================================")
        jobs = getPendingJob()

        db = client['gstservice']
        collection = db['recon_report']

        if jobs is not None and len(jobs) != 0:
            for i in range(len(jobs)):
                logging.info("Processing for job: " + str(jobs[i]))
                if "workspace_id" in jobs[i]:
                    workspacename = getWorkspaceName(jobs[i]['workspace_id'])
                    logging.info("Workspace Names: " + str(workspacename))
                    if workspacename is not None and len(workspacename) != 0:
                        filename, count = getData(workspacename)
                        logging.info("Filename: " + str(filename))
                        logging.info("Total no. of records: " + str(count))
                        if count != 0:
                            s3_url = uploadFile(filename)
                            logging.info("S3 URL: " + str(s3_url))
                            if s3_url is not None:
                                subject = ""
                                if "report_name" in jobs[i]:
                                    subject = subject + str(jobs[i]["report_name"]) + " is ready to download"
                                else:
                                    subject = "Report ready to download"

                                dynamic_template_data = {
                                    "subject": subject,
                                    "description": "As per your request we have generated this report of your workspace.",
                                    "download_link": s3_url,
                                }
                                template_id = "d-a6a5853662824aa7a69e990013cf1faa"
                                to_emails = []
                                if "to_emails" in jobs[i]:
                                    to_emails = jobs[i]["to_emails"]
                                if len(to_emails) != 0:
                                    sendMailToClient(to_emails, template_id, dynamic_template_data)
                                    key_to_check = {"_id": jobs[i]["_id"]}
                                    result = collection.update_one(
                                        key_to_check,
                                        {
                                            "$set": {
                                                "status": "COMPLETED",
                                                "link": s3_url,
                                                "total_record": count
                                            }
                                        })
                                    if result.matched_count > 0:
                                        logging.info("Updated the document: " + str(key_to_check))
                                    else:
                                        logging.info("No updates for the document: " + str(key_to_check))
                                else:
                                    key_to_check = {"_id": jobs[i]["_id"]}
                                    result = collection.update_one(
                                        key_to_check,
                                        {
                                            "$set": {
                                                "status": "TO MAIL MISSING",
                                                "total_record": count
                                            }
                                        })
                                    if result.matched_count > 0:
                                        logging.info("Updated the document: " + str(key_to_check))
                                    else:
                                        logging.info("No updates for the document: " + str(key_to_check))
                            else:
                                key_to_check = {"_id": jobs[i]["_id"]}
                                result = collection.update_one(
                                    key_to_check,
                                    {
                                        "$set": {
                                            "status": "FAILED TO GENERATE LINK",
                                            "total_record": count
                                        }
                                    })
                                if result.matched_count > 0:
                                    logging.info("Updated the document: " + str(key_to_check))
                                else:
                                    logging.info("No updates for the document: " + str(key_to_check))
                        else:
                            key_to_check = {"_id": jobs[i]["_id"]}
                            result = collection.update_one(
                                key_to_check,
                                {
                                    "$set": {
                                        "status": "COMPLETED",
                                        "total_record": count
                                    }
                                })
                            if result.matched_count > 0:
                                logging.info("Updated the document: " + str(key_to_check))
                            else:
                                logging.info("No updates for the document: " + str(key_to_check))
                    else:
                        key_to_check = {"_id": jobs[i]["_id"]}
                        result = collection.update_one(
                            key_to_check,
                            {
                                "$set": {
                                    "status": "WORKSPACE NOT FOUND"
                                }
                            })
                        if result.matched_count > 0:
                            logging.info("Updated the document: " + str(key_to_check))
                        else:
                            logging.info("No updates for the document: " + str(key_to_check))
                else:
                    key_to_check = {"_id": jobs[i]["_id"]}
                    result = collection.update_one(
                        key_to_check,
                        {
                            "$set": {
                                "status": "WORKSPACE ID MISSING"
                            }
                        })
                    if result.matched_count > 0:
                        logging.info("Updated the document: " + str(key_to_check))
                    else:
                        logging.info("No updates for the document: " + str(key_to_check))
        else:
            logging.info("No pending jobs")
        logging.info("======================================================")
    except Exception as e:
        logging.info("Exception happened in the main: " + str(e))
