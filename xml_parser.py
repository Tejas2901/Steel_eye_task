from asyncio.log import logger
from fileinput import filename
import requests
import wget
import xml.etree.ElementTree as Et
from zipfile import ZipFile
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError
import os
import logging


logging.basicConfig(level=logging.DEBUG)


xml_url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"


def download(url, file_name, type):
    """Downloads a file from the url and adds 
    it to the directory with the file name as 
    the given file_name

    Args:
        url (String): Url to download the file 
        file_name (String): Name of the file after the download
        type(String): Extension of the file, for XML files use xml 
    """    ''''''
    try:
        logger.info("Downloading the file from url")
        URL = url
        response = wget.download(URL, f"{file_name}.{type}")
        logger.info(f'{file_name}.{type}' + " has been added.")

    except:
        logger.error("Download failed, Check the URL again")


def xml_parser(xml_file):
    """Parses the given XML file

    Args:
        xml_file (String): name of the XML file

    Returns:
        Parsed XML file
    """
    try:
        if os.path.isfile(xml_file):
            logger.info("Parsing the xml_file")
            parsed_xml = Et.parse(xml_file)
            logger.info("Parsing completed")
            return Et.parse(xml_file)

    except:
        logger.error("File does not exists, Please check again")


def unzip(file_name):
    """Unzips the file and extracts all the contents in current directory

    Args:
        file_name (Zip file): Name of the Zip file present in the directory
    """
    try:
        if os.path.isfile(file_name):
            logger.info("Unzip the zip file")
            with ZipFile(file_name, 'r') as zipObj:
                # Extract all the contents of zip file in current directory
                zipObj.extractall()
            logger.info(
                "All the contents of the zip file have been added to the current directory.")
        else:
            logger.error("Something went wrong")

    except:
        logger.error("Zip file not present")


def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True

    except FileNotFoundError:
        print("The file was not found")
        return False

    except NoCredentialsError:
        print("Credentials not available")
        return False


# ***********************************************************************
#                           EXECUTION                                   *
# ************************************************************************


# download xml file
download(xml_url, "steel", "xml")

# parse the xml file
main_xml_tree = xml_parser("steel.xml")

# get the url to download the zip file
main_xml_root = main_xml_tree.getroot()
zip_url = (main_xml_root[1][0][1].text)

# download the zip file
download(zip_url, "zip_xml", "zip")
logger.info(
    "Zip file from the first link whose file type is  DLTINS has been downloaded")

# unzip the file
unzip('zip_xml.zip')

# parse the extracted xml
mytree_ = xml_parser("DLTINS_20210117_01of01.xml")
logger.info("Converting xml file to csv, Please wait")

# convert the xml to csv
df = pd.DataFrame()

# initializing rows
id, name, clt, cdi, nc, issr = [], [], [], [], [], []

# getting the root
myroot_ = mytree_.getroot()

# appending rows
for child in myroot_.iter():
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Id" and len(child.text) > 4:
        # There are two IDs columns in XML extracting only one
        id.append(child.text)
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}FullNm":
        name.append(child.text)
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}ClssfctnTp":
        clt.append(child.text)
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}CmmdtyDerivInd":
        cdi.append(child.text)
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}NtnlCcy":
        nc.append(child.text)
    if child.tag == "{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}Issr":
        issr.append(child.text)


file_string = "FinInstrmGnlAttrbts."

# Populating the columns
df[file_string + "Id"] = id
df[file_string + "FullName"] = name
df[file_string + "ClssfctnTp"] = clt
df[file_string + "CmmdtyDerivlnd"] = cdi
df[file_string + "NtnlCcy"] = nc
df[file_string + "Issr"] = issr

# Converting Dataframe to csv
try:
    df.to_csv("xml_file.csv", sep=",", index=False)
    logger.info("XML has been converted to csv and added to the directory")
    logger.info("Successful")
except:
    logger.error("Something went wrong")


ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXXXX'
SECRET_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
# to upload the csv to s3 bucket,enter the access key and secret key and then uncomment the below code

#uploaded = upload_to_aws('local_file', 'bucket_name', 's3_file_name')
