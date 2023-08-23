##########################################################################
# This script is DANGEROUS. It writes directly to the PowerSchool database
# Unfortunately this cannot be avoided, as you cannot import these via auto-comm

# USERS MUST HAVE THE "OLD LOGINS" POPULATED IN ORDER FOR THIS SCRIPT TO WORK
# THESE ARE: TeacherLoginID, TeacherLoginPW, LoginID, Password IN THE USERS TABLE


# The basic structure works as follows: we query all active staff members and go through one at a time
# Pass the user DCID to the table AccessTeacher and AccessAdmin to get their AccountIdentifier for each
# Query the PCAS_ExternalAccountMap table to see if an entry already exists for their AccountIdentifier
# If not, we need to insert an entry into the table which contains the login type, login provider, email, their AccountIdentifier, and the entry count for the table which we have to keep track of
# If it exists, make sure the email matches their current email, otherwise update it
# Repeat the process for both TEACHER and ADMIN login types

# See documentation for these tables:
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/users-ver7-8-0
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/accessteacher-ver7-0-0
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/accessadmin-ver7-0-0
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/pcas_externalaccountmap-ver10-1-0

# Needs the oracledb module to connect to the PS database

# importing modules
import oracledb  # used to connect to PowerSchool database
import datetime  # used to get current date and time
import os  # needed to get environement variables
from datetime import *

un = os.environ.get('POWERSCHOOL_WRITE_USER') # username for read/write user in PowerSchool
pw = os.environ.get('POWERSCHOOL_WRITE_DB_PASSWORD') # password for write user
cs = os.environ.get('POWERSCHOOL_PROD_DB') # connection string containing IP address, port, and database name to connect to

print("Username: " + str(un) + " |Password: " + str(pw) + " |Server: " + str(cs)) #debug so we can see where oracle is trying to connect to/with

if __name__ == '__main__': # main file execution
    with oracledb.connect(user=un, password=pw, dsn=cs) as con: # create the connecton to the database
        with con.cursor() as cur:  # start an entry cursor
            with open('staffSSOLog.txt', 'w') as log:
                startTime = datetime.now()
                startTime = startTime.strftime('%H:%M:%S')
                print(f'Execution started at {startTime}')
                print(f'Execution started at {startTime}', file=log)






                endTime = datetime.now()
                endTime = endTime.strftime('%H:%M:%S')
                print(f'INFO: Execution ended at {endTime}')
                print(f'INFO: Execution ended at {endTime}', file=log)