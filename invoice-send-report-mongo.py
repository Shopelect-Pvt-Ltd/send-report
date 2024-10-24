import time
from tendo import singleton
import os
from dotenv import load_dotenv
load_dotenv()
import logging
from logging.handlers import TimedRotatingFileHandler
from pymongo import MongoClient
import boto3
import requests
from sendgrid.helpers.mail import Mail
from datetime import datetime
import pytz
import base64
import requests
from bs4 import BeautifulSoup
import shutil
import hashlib
import re

ist = pytz.timezone('Asia/Kolkata')
folder_path = "log/"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)


log_handler = TimedRotatingFileHandler(
    folder_path + 'invoice-report-mongo.log',  # Base file name (rotation will append numbers automatically)
    when='D',  # Rotate by day
    interval=1,  # Rotate every 1 day
    backupCount=7,  # Keep only the last 7 log files
    encoding='utf-8',
    delay=False
)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')
log_handler.setFormatter(formatter)
logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler]
)

aws_access_key_id = os.getenv('AWS_ACCESS')
aws_secret_access_key = os.getenv('AWS_SECRET')
bucket_name = os.getenv('DEST_AWS_BUCKET_NAME')

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')

MONGO_URL = os.getenv('MONGO_URL')
client = MongoClient(MONGO_URL, maxIdleTimeMS=None)
logging.info("Mongo connection successful")

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


def deleteFolder(folder_path):
    logging.info("deleteFolder called...")
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info(f"Folder '{folder_path}' deleted successfully.")
    else:
        logging.info(f"Folder '{folder_path}' does not exist.")


def uploadFile(filepath):
    try:
        logging.info("uploadFile called...")
        filename = filepath.split("/")[-1]
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
        object = f"{bucket_time}/{filename}"
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{object}"
        s3.upload_file(filepath,
                       bucket_name,
                       Key=object
                       )
        if os.path.exists(filepath):
            os.remove(filepath)
        return s3_url
    except Exception as e:
        logging.info("Exception happened in the getS3Url: " + str(e))
        if os.path.exists(filepath):
            os.remove(filepath)
        return None



def findFileHash(file_path, hash_algo='sha256'):
    hash_func = hashlib.new(hash_algo)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def zipHandler(local_file_path):
    try:
        logging.info("zipHandler called...")
        shutil.make_archive(local_file_path, 'zip', local_file_path)
        filehash = findFileHash(local_file_path + ".zip")
        s3_link = uploadFile(local_file_path + ".zip")
        deleteFolder(local_file_path)
        return s3_link, filehash
    except Exception as e:
        logging.info("Exception happened in zipHandler: " + str(e))
        return None

def fetch_base64_from_page(url):
    element_id = 'downloadLink'
    # Fetch the HTML content from the URL
    response = requests.get(url)
    response.raise_for_status()

    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the <a> tag with the specified ID and get the href attribute
    anchor = soup.find('a', id=element_id)
    if anchor and 'href' in anchor.attrs:
        return anchor['href']
    else:
        return None

def download_base64_file(base64_string, file_path):
    logging.info("Downloading the file: " + str(file_path))
    # Extract the MIME type and base64 data from the input string
    mime_info, base64_data = base64_string.split(',', 1)

    # Decode base64 to binary data
    binary_data = base64.b64decode(base64_data)

    # Save the binary data to a file
    with open(file_path, 'wb') as file:
        file.write(binary_data)


def downloadFile(baseFolderName, invoiceLinks, filePathArr):
    try:
        logging.info("downloadFile called...")
        logging.info("No. of file to download: " + str(len(invoiceLinks)))
        for i in range(len(invoiceLinks)):
            url = invoiceLinks[i]
            base64_string = fetch_base64_from_page(url)
            mime_type = base64_string.split(';')[0].split(':')[1]
            mime_to_extension = {
                'application/pdf': '.pdf',
                'image/png': '.png',
                'image/jpeg': '.jpg',
                'text/plain': '.txt',
                'text/html':'.html',
                'application/zip': '.zip',
            }
            # file_extension = mime_to_extension.get(mime_type, '.bin')
            file_extension = mime_to_extension.get(mime_type, '.pdf')
            filehash = url.split("/")[-1]
            filename = ""
            filepath = baseFolderName + "/"
            for j in range(len(filePathArr)):
                filename=filePathArr[j]
                filepath += filename
                if j < len(filePathArr):
                    filename += "_"
                    filepath += "/"
            filename += filehash
            file_path_with_filename = f"{filepath}{filename}{file_extension}"
            download_base64_file(base64_string, file_path_with_filename)

    except Exception as e:
        logging.info("Exception happened in downloadFile: " + str(e))

def getInvoicesDetails(baseFolderName, folderDetails):
    try:
        logging.info("getInvoicesDetails called...")
        totalfiles = 0
        for i in range(len(folderDetails)):
            filePathArr=[]
            folderName = folderDetails[i]["_id"]
            folderName = re.sub(r'[^A-Za-z0-9 ]+', '', folderName)
            folderName = folderName.replace(' ', '_')
            filePathArr.append(folderName)
            if len(folderDetails[i]["invoice_links"])!=0:
                downloadFile(baseFolderName, folderDetails[i]["invoice_links"], filePathArr)
                totalfiles+=len(folderDetails[i]["invoice_links"])
                break
        s3_link, filehash = zipHandler(baseFolderName)
        return s3_link, filehash, totalfiles
    except Exception as e:
        logging.info("Exception happened in getInvoicesDetails: " + str(e))
        return None, None, None


def createFolders(folderDetails, base_dir='download/invoice_folders_'+str(currtime)):
    logging.info("createFolders called...")
    data = []
    for i in range(len(folderDetails)):
        folderName = folderDetails[i]["_id"]
        if folderName:
            folderName = re.sub(r'[^A-Za-z0-9 ]+', '', folderName)
            folderName = folderName.replace(' ', '_')
            data.append([folderName])

    # Create the base directory if it doesn't exist
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    for entry in data:
        # The first element is always the airline name
        parentfolder = entry[0]
        # All remaining elements are subfolders
        subfolders = entry[1:]

        # Construct the path recursively by joining airline and subfolders
        folder_path = os.path.join(base_dir, parentfolder, *subfolders)

        # Create the folders if they don't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
    logging.info(f"Folders created under {base_dir}")
    return base_dir


def getFolderGrouping(rowGroupColumns,columnLinks, database, table):
    try:
        logging.info("getFolderGrouping called...")
        db = client[database]
        collection = db[table]
        aggregation_pipeline=[
            {
                "$group": {
                    "_id": '$'+str(rowGroupColumns[0]),
                    "invoice_links": {
                        "$push": '$'+str(columnLinks[0])
                    }
                }
            },
            {
                "$project": {
                    "invoice_links": 1
                }
            }
        ]
        result = list(collection.aggregate(aggregation_pipeline))
        return result
    except Exception as e:
        logging.info("Exception happen in getFolderGrouping:  "+str(e))
        return []

def getPendingJob():
    logging.info("getPendingJob called...")
    db = client['gstservice']
    collection = db['invoice_report']
    # result = list(collection.find({"reportId": "09db4a69-c8c1-4470-930b-cd299c71479d"}).limit(LIMIT))
    result = list(collection.find({"status": "PENDING","dbType":"mongodb"}).sort({"createdBy": -1}).limit(LIMIT))
    return result

def removeOldFilesFolder():
    logging.info("removeOldFilesFolder called...")
    folder_path = 'download/'

    if os.path.exists(folder_path):
        for item in os.listdir(folder_path):
            item_path = os.path.join(folder_path, item)

            if os.path.isfile(item_path):
                os.remove(item_path)
                logging.info(f"File removed: {item_path}")

            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
                logging.info(f"Folder removed: {item_path}")

        logging.info("All files and folders deleted.")
    else:
        logging.info("The specified folder does not exist.")


def statusUpdater(key_to_check,update):
    result = collection.update_one(key_to_check,update)
    if result.matched_count > 0:
        logging.info("Updated the document: " + str(key_to_check)+" Update: "+str(update))
    else:
        logging.info("No updates for the document: " + str(key_to_check)+" Update: "+str(update))

if __name__ == '__main__':
    try:
        me = singleton.SingleInstance()
        logging.info("======================================================")
        removeOldFilesFolder()
        jobs = getPendingJob()
        db = client['gstservice']
        collection = db['invoice_report']
        if jobs is not None and len(jobs) != 0:
            for i in range(len(jobs)):
                logging.info("Processing for job: " + str(jobs[i]))
                key_to_check = {"_id": jobs[i]["_id"]}
                update={
                    "$set":
                             {
                                 "status": "IN PROGRESS"
                             }
                    }
                statusUpdater(key_to_check, update)

                if "groupingPayload" in jobs[i] and "rowGroupColumns" in jobs[i]["groupingPayload"]  and  len(jobs[i]["groupingPayload"]["rowGroupColumns"])!=0 and "columnLinks" in jobs[i] and len(jobs[i]["columnLinks"])!=0:
                    folderDetails = getFolderGrouping(jobs[i]["groupingPayload"]["rowGroupColumns"], jobs[i]["columnLinks"], jobs[i]["database"],jobs[i]["table"])
                    if folderDetails is None or len(folderDetails)==0:
                        key_to_check = {"_id": jobs[i]["_id"]}
                        update = {
                            "$set": {
                                "status": "INCORRECT DETAILS",
                            }
                        }
                        statusUpdater(key_to_check, update)
                    else:
                        baseFolderName = 'download/invoice_folders_' + str(currtime)
                        if "report_name" in jobs[i]:
                            baseFolderName = 'download/' + str(jobs[i]["report_name"])
                        createFolders(folderDetails, baseFolderName)
                        s3_url, filehash, totalfiles = getInvoicesDetails(baseFolderName, folderDetails)
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
                                "download_link": "https://files.finkraft.ai/report-" + str(filehash),
                            }
                            template_id = "d-a6a5853662824aa7a69e990013cf1faa"
                            to_emails = []
                            if "to_emails" in jobs[i]:
                                to_emails = jobs[i]["to_emails"]
                            if len(to_emails) != 0:
                                sendMailToClient(to_emails, template_id, dynamic_template_data)
                                key_to_check = {"_id": jobs[i]["_id"]}
                                update = {
                                    "$set": {
                                        "status": "COMPLETED",
                                        "link": s3_url,
                                        "filehash": filehash,
                                        "totalfiles": totalfiles
                                    }
                                }
                                statusUpdater(key_to_check, update)
                            else:
                                key_to_check = {"_id": jobs[i]["_id"]}
                                update = {
                                    "$set": {
                                        "status": "COMPLETED MAIL MISSING",
                                        "link": s3_url,
                                        "filehash": filehash,
                                        "totalfiles": totalfiles
                                    }
                                }
                                statusUpdater(key_to_check, update)
                else:
                    key_to_check = {"_id": jobs[i]["_id"]}
                    update = {
                        "$set": {
                            "status": "FAILED",
                        }
                    }
                    statusUpdater(key_to_check, update)
        else:
            logging.info("No pending jobs")
        logging.info("======================================================")
    except Exception as e:
        logging.info("Exception happened in the main: " + str(e))
