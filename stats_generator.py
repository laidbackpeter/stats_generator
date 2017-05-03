# !/usr/bin/python2.7
# coding=utf-8
# -*- coding: utf-8 -*-
# Stats Gen
# Muchina

import smtplib
import psycopg2
import ConfigParser
import logging
import urllib2
import json
import time
import datetime
import os
import sys
import csv
import mimetypes
from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from string import Template
# In memory DB
from pydblite.pydblite import Base
from datetime import datetime


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s', filename='/opt/scripts/stats_gen/logs/stats_gen.log')
log = logging.getLogger("reminder")

config = ConfigParser.ConfigParser()
config.readfp(open('/opt/scripts/stats_gen/config.ini'))

db_host = config.get('db', 'db_host')
db_port = config.get('db', 'db_port')
db_username = config.get('db', 'db_username')
db_password = config.get('db', 'db_password')
db_name = config.get('db', 'db_name')

# Others
sleep_int = config.get('others', 'sleep_interval')

# Email
recep_list = config.get('email', 'recep_list')


def db_conn():
    global cursor, db
    try:
        db = psycopg2.connect(database=db_name, user=db_username, host=db_host, password=db_password, port=db_port)
        cursor = db.cursor()
        log.info("Connected to " + db_name + " on IP " + db_host)
    except Exception, e:
        log.error("!dbConnection : "+format(e.message))


def get_time_range():
    global start_time, end_time
    end_time = str(datetime.now())
    db_path = '/opt/scripts/stats_gen/last.db'
    db = Base(db_path)
    check = db.exists()
    if check:
        log.info("In-memory DB exists. Path - " + db_path)
        db.open()
        record = db(fk_index=1)
        # Get first record from list
        start_time = record[0]
        # Using id as key of the map, get the value
        start_time = start_time['last_time']
        # Update last time
        db.update(record, last_time=end_time)
        db.commit()
        record = db(fk_index=1)
        log.info('Start Time: ' + start_time)
        log.info('Start Time: ' + end_time)
    else:
        log.info("Creating in-memory DB. Path - " + db_path)
        db.create('fk_index', 'last_time')
        # Insert records for next read
        db.insert(last_time=end_time, fk_index=1)
        db.commit()
        start_time = str(datetime.now().strftime("%Y-%m-%d 00:00:00.000000"))
        record = db(fk_index=1)
        log.info('Start Time: ' + start_time)
        log.info('Start Time: ' + end_time)


def get_data_without_params(stats_query):
    try:
        cursor.execute(stats_query)
        return cursor.fetchone()
    except Exception, e:
        log.error("!get_data_without_params : " + format(e.message))


def delete_file(file_name):
    try:
        os.remove(file_name)
        log.info('Deleting - ' + file_name)
    except Exception, e:
        log.error("Error :" + format(e.message))


def get_data():
    lending_txn_count = "select count(*) from tbl_loans where loan_time >= '%s' and loan_time < '%s'" % (start_time, end_time)
    lbn_count = "select count(*) from tbl_auto_sms_notifications where sms_timestamp >= '%s' and sms_timestamp < '%s' and sms_message='not-subscribed'" % (start_time, end_time)
    principal_recovery_count = "select count(*) from tbl_loans_repay where event_time >= '%s' and event_time < '%s' and cents_principal > 0" % (start_time, end_time)
    principal_recovery_amt = "select coalesce((sum(cents_principal)/100),0) from tbl_loans_repay where event_time >='%s' and event_time < '%s' and cents_principal > 0" % (start_time, end_time)
    service_fee_amt = "select coalesce((sum(cents_serviceq)/100),0) from tbl_loans_repay where event_time >='%s' and event_time < '%s' and cents_serviceq > 0" % (start_time, end_time)

    result_lending_txn_count = get_data_without_params(lending_txn_count)
    result_lbn_count = get_data_without_params(lbn_count)
    result_principal_recovery_count = get_data_without_params(principal_recovery_count)
    result_principal_recovery_amt = get_data_without_params(principal_recovery_amt)
    result_service_fee_amt = get_data_without_params(service_fee_amt)

    # generating file name
    st = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S.%f')
    stf = st.strftime('%Y%m%d%H%M%S%f')
    fn = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S.%f')
    fnf = fn.strftime('%Y%m%d%H%M%S%f')
    filename = str(stf) + '_' + str(fnf) + '_OOC_Data.csv'
    log.info('File name - ' + filename)

    # Print records
    # print 'Period_start_timestamp', 'Period_end_timestamp', 'lending_txs', 'LBN_counts', 'Principal_recovery_txs', 'Principal_recovery_amount', 'Service_fee_amount'
    # print result_lending_txn_count[0], result_lbn_count[0], result_principal_recovery_count[0], result_principal_recovery_amt[0], result_service_fee_amt[0]

    # Generating file
    csv_file = '/tmp/' + filename
    try:
        f = open(csv_file, 'w')
        c = csv.writer(f)
        # Write headers
        c.writerow(('Period_start_timestamp', 'Period_end_timestamp', 'lending_txs', 'LBN_counts', 'Principal_recovery_txs', 'Principal_recovery_amount', 'Service_fee_amount'))
        # Write data
        c.writerow((start_time,end_time,result_lending_txn_count[0], result_lbn_count[0], result_principal_recovery_count[0], result_principal_recovery_amt[0], result_service_fee_amt[0]))
    except Exception, e:
        log.error("Error :" + format(e.message))
    finally:
        f.close()
    # send file
    message = "Hi, attached is the file containing the generated statistics. In case of any queries kindly contact test@gmail.com"
    print csv_file
    sendEmail(message, recep_list, csv_file, filename)
    log.info('File sent')
    # Delete file
    delete_file(csv_file)


def sendEmail(message, emails, attachment, filename):
    smtpObj = smtplib.SMTP('x.x.x.x', 25)
    smtpObj.set_debuglevel(True)
    outer = MIMEMultipart('mixed')
    sender = "OOC Stats Generator"
    recipients = emails.split(',')
    outer['Subject'] = "OOC Stats"
    outer['From'] = sender
    outer['To'] = ", ".join(recipients)
    textpart = MIMEText(message, 'html')
    outer.attach(textpart)
    ctype, encoding = mimetypes.guess_type(attachment)
    if ctype is None or encoding is not None:
        # No guess could be made, or the file is encoded (compressed), so
        # use a generic bag-of-bits type.
        ctype = 'application/octet-stream'
    maintype, subtype = ctype.split('/', 1)
    if maintype == 'text':
        fp = open(attachment)
        # Note: we should handle calculating the charset
        msg = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'image':
        fp = open(attachment, 'rb')
        msg = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'audio':
        fp = open(attachment, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(attachment, 'rb')
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()
        # Encode the payload using Base64
        encoders.encode_base64(msg)
    msg.add_header('Content-Disposition', 'attachment', filename=filename)
    outer.attach(msg)

    try:
        smtpObj.sendmail(sender, recipients, outer.as_string())
        log.info('Emails sent')
        print "Sent successfully"
        # smtpObj.quit()
    except smtplib.SMTPException, e:
        print "Error: Unable to Send"
        log.error("Error :" + format(e.message))


def main():
    try:
        while True:
            db_conn()
            log.info('Generating')
            get_time_range()
            get_data()
            log.info('Sleeping')
            time.sleep(int(sleep_int))
            db.close()
    except Exception, e:
        print e.message
        log.error("Error :" + format(e.message))


# Run the main function
main()
