import os
import json
import sys
import dropbox
import pymysql
import smtplib
import boto3
import pandas as pd
import numpy as np

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from twisted.enterprise import adbapi
from sqlalchemy import *

from common import helpers

pymysql.install_as_MySQLdb()


# S3 upload
helpers.upload_file_s3('./output/fst_sig_not.csv', 'scripts/fst_sig_not.csv')