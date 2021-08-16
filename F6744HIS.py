# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 22:22:34 2020
@author: aaggar15
"""
import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import mysql.connector as sql
import configparser as cp
from datetime import datetime, date
import os 
import smtplib
import time
from logger import get_logger
from ness_logging import NessLogging
from ecapEnum import Severity, Operation

log = get_logger('root')

FORMAT = "[%(levelname)s: %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
log.setLevel(logging.DEBUG)

ness_obj : NessLogging = None #for ness logging

def make_sql_conn():
    log.info("creating SQL connection")
    hst=os.getenv('sql_host')
    usr=os.getenv('sql_user')
    passwd=os.getenv('sql_password')
    db=os.getenv('sql_database')
    connect_attempts = 0
    retry_flag = True
    while retry_flag and connect_attempts < 3:
        try:
            global mydb
            mydb = sql.connect(host=hst,user=usr,password=passwd,database=db)
            retry_flag = False
        except Exception as e:
            msg = f"Something went wrong while creating mysql connection, attempt - {connect_attempts + 1}, error - {e}"
            log.info(msg)
            log.info("Connection retry will be attempted after 30 seconds")
            ness_obj.post(msg=msg, name="make_sql_conn", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
            time.sleep(30)
            connect_attempts = connect_attempts + 1

    if retry_flag == False:
        log.info("My SQL connection created")
    else:
        msg = f"something went wrong while creating SQL connection - All retries have failed. Host = {hst}, username= {usr}, password = {passwd}, database = {db}"
        log.error(msg)
        ness_obj.post(msg=msg, name="make_sql_conn", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while making sql connection. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0) 
    
def get_min_proc_prd():
    log.info("Getting Minimum Proc Period")
    try:
        tbnm=os.getenv('hpartnxref')
        dbnm=os.getenv('sql_database')
        mycursor = mydb.cursor()
        sql=("select min(proc_prd) from {}.{}".format(dbnm,tbnm))
        mycursor.execute(sql)
        results = mycursor.fetchone()
        global minprocprd
        minprocprd = results[0]
        log.info('minprocprd - %s', minprocprd)
    except Exception as e:
        msg = f'Something went wrong while getting minimum proc period message = {str(e)}'
        log.error(msg)
        ness_obj.post(msg=msg, name="get_min_proc_prd", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while getting minimum proc period. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
        
def get_h_partn_xref(minprocprd):
    try:
        log.info("Getting Partition ID corresponding to the minimum Proc Period - %s",minprocprd)
        tbnm=os.getenv('hpartnxref')
        dbnm=os.getenv('sql_database')
        sql=("SELECT PROC_PRD, H_PARTN_ID FROM {}.{} WHERE PROC_PRD = %s".format(dbnm,tbnm))
        mycursor = mydb.cursor()
        mycursor.execute(sql,(minprocprd,))
        results = mycursor.fetchone()
        global procprd
        global hprtid
        procprd = results[0]
        hprtid  = results[1]
        log.info('The minimum proc period is - %s', procprd)
        log.info('The hprtid corresponding to the PROC PRD - %s is - %s',procprd, hprtid)
    except Exception as e:
        msg = f'Something went wrong while getting Partition ID for minimum proc period message = {str(e)}'
        log.error(msg)
        ness_obj.post(msg=msg, name="get_h_partn_xref", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while getting partition ID corresponding to minimum Proc Period. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    finally:
        mycursor.close()
        
def process_proc_prd(procprd):
    try:
        log.info('Calculating number of months from the oldest Partition to current month')
        procdt = procprd + '01'
        procdate1 = datetime.strptime(procdt, '%Y%m%d').strftime('%Y-%m-%d')
        procdate = datetime.strptime(procdate1, '%Y-%m-%d')
        #log.info('procdt - %s', procdt)
        #log.info('procdate - %s', procdate)
        currdate = date.today()
        global num_of_months
        num_of_months = ((currdate.year - procdate.year) * 12 + (currdate.month - procdate.month))
        log.info('This Partition is %s months old', num_of_months)    
    except Exception as e:
        msg = f'Error when calculating the num of months message = {str(e)}' 
        log.error(msg)
        ness_obj.post(msg=msg, name="process_proc_prd", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while calculating number of months. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    
def del_h_int_mbr(hprtid):
    try:
        log.info('Truncating the partition from H_INT_MBR table - Partition No - %s',hprtid)
        dbnm=os.getenv('sql_database')
        tbnm=os.getenv('hintmbr')
        sql=("Alter table {}.{} Truncate Partition P%s".format(dbnm,tbnm))
        mycursor = mydb.cursor()
        mycursor.execute(sql,(hprtid,))
    except Exception as e:
        msg = f'Error while deleting data from H_INT_MBR message = {str(e)}, Partition = {hprtid}'
        log.error(msg)
        ness_obj.post(msg=msg, name="del_h_int_mbr", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while truncating H_INT_MBR table. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    finally:
        log.info('Truncate Successfull in H_INT_MBR for Partition - %s', hprtid)

def del_h_int_mbr_cov(hprtid):
    try:
        log.info('Truncating the partition from H_INT_MBR_COV table - Partition No - %s',hprtid)
        dbnm=os.getenv('sql_database')
        tbnm=os.getenv('hintmbrcov')
        sql=("Alter table {}.{} Truncate Partition P%s".format(dbnm,tbnm))        
        mycursor = mydb.cursor()
        mycursor.execute(sql,(hprtid,))
    except Exception as e:
        msg = f'Error while deleting data from H_INT_MBR_COV message = {str(e)}, Partition = {hprtid}'
        log.error(msg)
        ness_obj.post(msg=msg, name="del_h_int_mbr_cov", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while truncating H_INT_MBR_COV table. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    finally:
        log.info('Truncate Successfull in H_INT_MBR_COV for Partition - %s', hprtid)

def calc_new_proc_prd(hprtid):
    try:
        log.info('Fetching Current maximum proc period from H_PARTN_XREF table to update the truncated partition')
        dbnm=os.getenv('sql_database')
        tbnm=os.getenv('hpartnxref')
        sql=("Select max(proc_prd) from {}.{}".format(dbnm,tbnm))    
        mycursor = mydb.cursor()
        mycursor.execute(sql)
        results = mycursor.fetchone()
        maxproc = results[0]
        log.info('The current Max Proc period in H_PARTN_XREF table is - %s', maxproc)
        year = maxproc[0:4]
        month = maxproc[4:6]
        if (int(month) == 12):
            year = int(year) + 1
            month = '01'
            global newprocprd
            newprocprd = str(year) + str(month)
            log.info('The newprocprd for Partition - %s is - %s', hprtid, newprocprd)
        else:
            newprocprd = int(maxproc) + 1
            newprocprd = str(newprocprd)
            log.info('The newprocprd for Partition - %s is - %s', hprtid, newprocprd)
    except Exception as e:
        msg = f'Error while calculating new proc period message = {str(e)}'
        log.error(msg)
        ness_obj.post(msg=msg, name="calc_new_proc_prd", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while Calculating new Proc Period. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    finally:
        log.info('New Proc Period Calculated Successfully for partition - %s', hprtid)
 
def updt_proc_prd(newprocprd,hprtid):       
    try:
        log.info('Updating the new proc period for partition - %s in H_PARTN_XREF table',hprtid)
        dbnm=os.getenv('sql_database')
        tbnm=os.getenv('hpartnxref')
        ts = time.time()
        timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        sql=("UPDATE {}.{} SET PROC_PRD = %s, UPDT_USER_ID = %s, updt_dttm = %s where H_PARTN_ID = %s".format(dbnm,tbnm))    
        mycursor = mydb.cursor()
        mycursor.execute(sql,(newprocprd,'PYTHON',timestamp,hprtid,))
        mydb.commit()
    except Exception as e:
        msg = f'Error while updating new proc period message = {str(e)}, partition - {hprtid}'
        log.error(msg)
        ness_obj.post(msg=msg, name="updt_proc_prd", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'History Table Truncation Process Failed'
        mail_body='Hi Team, \n\nThe History Table Truncation Process has failed while Updating Proc Period in H_PARTN_XREF table. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
        sys.exit(0)
    finally:  
        log.info('The Proc Period - %s has been updated Successfully for partition - %s in H_PARTN_XREF table',newprocprd,hprtid)

def send_mail(mail_sub,mail_body):
    log.info("Sending mail to the distribution list")
    mail_from=os.getenv('from_mail')
    mail_to=os.getenv('to_mail')
    mail_server=os.getenv('mserver')
    mail_subject=mail_sub
    try:
        message = """From: %s\r\nTo: %s\r\nSubject: %s\r\n\

        %s
        """ % (mail_from,mail_to, mail_subject, mail_body)
        
        server = smtplib.SMTP(mail_server, 25)
        server.sendmail(mail_from, mail_to, message)
        server.quit()

    except Exception as e:
        msg = f'Something went wrong while sending failure mail to distribution message= {e}'
        log.error(msg)
        ness_obj.post(msg=msg, name="send_mail", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        sys.exit(0)

def fetch_ness_logging_url():
    global ness_obj
    ness_obj = NessLogging(os.getenv('ness_logging_url'))

# Calling the main functions:
if __name__ == "__main__":        

# Fetching ness logging url 
    fetch_ness_logging_url()

#Making the SQL Connection
    make_sql_conn()

# Call to fetch the minimum process period present in H_PARTN_XREF table

    get_min_proc_prd()

#Call to fetch the Partition ID corresponding to the minimum Proc Period

    get_h_partn_xref(minprocprd)   

#Call Function to check if the oldest partition is 10 years old or not

    process_proc_prd(procprd)

# If the partition is more than 10 years old, go ahead and perform the delete from history tables
    archv_months = int(os.getenv('archvmonths'))

    if (num_of_months > archv_months):
        log.info('This partition needs to be Truncated from the History Tables')
    
#Call function to delete the data from H_INT_MBR table
    
        del_h_int_mbr(hprtid)
    
#Call function to delete the data from H_INT_MBR_COB table
    
        del_h_int_mbr_cov(hprtid)

#Call Function to get the new Proc Prd to be updated in the H_PARTN_XREF table
    
        calc_new_proc_prd(hprtid)

#Call Function to update the new Proc Prd
    
        updt_proc_prd(newprocprd,hprtid)
        msg = f'The Partition - {hprtid} has been truncated Successfully for the History Tables'        
        log.info(msg)
        ness_obj.post(msg=msg, name="Process Completion", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'Partition - {hprtid} Truncated Successfully for the History Tables'   
        mail_body=f'Hi Team, \n\nThe History Table Truncation has Completed Successfully for partition - {hprtid}. \n\n The new Proc Period is {newprocprd} for partition - {hprtid}. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
    
    else:
    
# Apply logging here and upar main function create karna h 
        msg = f'No Truncation and update needed for The Partition - {hprtid}'
        log.info(msg)
        ness_obj.post(msg=msg, name="No Truncation and Update", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
        mail_sub=f'Alert! Partition Processing - {hprtid} - Partition Not Truncated for the History Tables'   
        mail_body=f'Hi Team, \n\n Partition - {hprtid} Not Truncated for the History Tables. \n\n Please Check H_PARTN_XREF table in MYSQL to verify. \n \n \nThanks & Regards \nTruncation job'
        send_mail(mail_sub,mail_body)
else:
    msg = f'no calling function'
    log.info(msg)
    ness_obj.post(msg=msg, name="No Calling Function", severity=Severity.ERR.name, reason="nonSecurity",
                                operation=Operation.READ.name)
    mail_sub=f'History Table Truncation Process Failed'
    mail_body='Hi Team, \n\nThe History Table Truncation Process has failed as there was no main function. \n\nPlease Check the logs on kubernetes for more details. \n \n \nThanks & Regards \nTruncation job'
    send_mail(mail_sub,mail_body)
    sys.exit(0)
    
