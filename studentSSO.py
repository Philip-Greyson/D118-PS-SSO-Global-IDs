##########################################################################
# This script is DANGEROUS. It writes directly to the PowerSchool database
# Unfortunately this cannot be avoided, as you cannot import these via auto-comm

# USERS MUST HAVE THE "OLD LOGINS" POPULATED IN ORDER FOR THIS SCRIPT TO WORK
# THESE ARE: Student_Web_ID and Student_Web_Password IN THE STUDENTS TABLE


# The basic structure works as follows: we query all studnts and go through one at a time
# Pass the student DCID to the table AccessStudent table to get their AccountIdentifier
# Query the PCAS_ExternalAccountMap table to see if an entry already exists for their AccountIdentifier
# If not, we need to insert an entry into the table which contains the login type, login provider, email, their AccountIdentifier, and the entry count for the table which we have to keep track of
# If it exists, make sure the email matches their current email, otherwise update it

# See documentation for these tables:
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/students-1-ver3-6-1
# https://ps.powerschool-docs.com/pssis-data-dictionary/latest/accessstudent-ver7-0-0
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

# DELETE THIS LIST BELOW IF YOU ARE NOT IN D118
brokenDCIDs = [] # list of "broken" DCIDs, these users dont show up in powerschool search even searching by users_dcid, are old and non-functioning duplicates of real accounts

if __name__ == '__main__': # main file execution
    with oracledb.connect(user=un, password=pw, dsn=cs) as con: # create the connecton to the database
        with con.cursor() as cur:  # start an entry cursor
            with open('studentSSOLog.txt', 'w') as log:
                startTime = datetime.now()
                startTime = startTime.strftime('%H:%M:%S')
                print(f'Execution started at {startTime}')
                print(f'Execution started at {startTime}', file=log)

                # Start with the query of all users
                cur.execute('SELECT dcid, student_number, schoolid FROM students WHERE enroll_status = 0 ORDER BY student_number DESC')
                users = cur.fetchall()
                for user in users:
                    dcid = int(user[0])
                    email = str(int(user[1])) + '@d118.org' # CHANGE THIS IF YOU ARE NOT AT D118
                    homeschool = int(user[2])
                    try: # put overall processing in try/except blocks so we can just skip a user on an error and continue
                        if dcid not in brokenDCIDs: # only process users who are not in the brokenDCID list
                            print(f'INFO: Starting user {email} at building {homeschool} with DCID: {dcid}')
                            print(f'INFO: Starting user {email} at building {homeschool} with DCID: {dcid}', file=log)
                            
                            # GET THE PCAS_EXTERNAL TABLE UNIQUE COUNTER
                            cur.execute('SELECT PCAS_ExternalAccountMapID FROM PCAS_ExternalAccountMap ORDER BY PCAS_ExternalAccountMapID DESC') # get the internal counter, sort by descending so most recent value is first
                            counters = cur.fetchall()
                            maxEntry = int(counters[0][0])
                            newEntry = maxEntry+1
                            # print(f'\tDEBUG: The old maximum entry in the PCAS_ExternalAccountMapID field was {maxEntry}, setting new entry to {newEntry}') # debug
                            # print(f'\tDEBUG: The old maximum entry in the PCAS_ExternalAccountMapID field was {maxEntry}, setting new entry to {newEntry}', file=log) # debug

                            # START OF THE STUDENT ACCESS BLOCK
                            cur.execute('SELECT AccountIdentifier FROM AccessStudent WHERE StudentsDCID = :studentDCID', studentDCID=str(dcid)) # using bind variable as shown as best practice here: https://python-oracledb.readthedocs.io/en/latest/user_guide/sql_execution.html#fetch-methods
                            studentAccessResults = cur.fetchall()
                            if studentAccessResults: # if we found a result, their old login info should be populated
                                studentIdentifier = studentAccessResults[0][0]
                                # print(f'\t\tDEBUG: Student identifier for {dcid} is {studentIdentifier}') # debug
                                # print(f'\t\tDEBUG: Stduent identifier for {dcid} is {studentIdentifier}', file=log) # debug

                                cur.execute("SELECT OpenIdUserAccountID FROM PCAS_ExternalAccountMap WHERE PCAS_AccountToken = :studentToken", studentToken =studentIdentifier) # search for an existing PCAS_External account map account
                                pcasAccounts = cur.fetchall()
                                # print(f'\t\t\t\tDEBUG: PCAS STUDENT: {pcasAccounts}') # debug
                                # print(f'\t\t\t\tDEBUG: PCAS STUDENT: {pcasAccounts}', file=log) # debug
                                if pcasAccounts: # if the result exists, they already have an account
                                    currentPCASEmail = pcasAccounts[0][0]
                                    # print(f'\t\t\tINFO: PCAS STUDENT Account found, currently set to {currentPCASEmail}')
                                    # print(f'\t\t\tINFO: PCAS STUDENT Account found, currently set to {currentPCASEmail}', file=log)
                                    if currentPCASEmail != email:
                                        print(f'ACTION: Current PCAS STUDENT Email {currentPCASEmail} does not match PS Email {email}, UPDATING')
                                        print(f'ACTION: Current PCAS STUDENT Email {currentPCASEmail} does not match PS Email {email}, UPDATING', file=log)
                                        try:
                                            # create a string with the SQL Update that uses bind variables to pass the variables. See here: https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html#bind
                                            updateSQL = "UPDATE PCAS_ExternalAccountMap SET OpenIdUserAccountID = :email, ApplicationUserType = :AccountType, OpenIDIssuerURL = :URL WHERE PCAS_AccountToken = :studentToken"
                                            cur.execute(updateSQL, [email, "STUDENT", "https://accounts.google.com", studentIdentifier]) # execute the update, pass the new email, type, identity provider, and student identifier
                                            con.commit()
                                        except Exception as err:
                                            print(f'ERROR on update of PCAS STUDENT Account for user {email} with DCID {dcid}: {err}')
                                            print(f'ERROR on update of PCAS STUDENT Account for user {email} with DCID {dcid}: {err}', file=log)
                                else:
                                    print(f'ACTION: No STUDENT Global ID found for {email} with DCID {dcid}, will try creating one')
                                    print(f'ACTION: No STUDENT Global ID found for {email} with DCID {dcid}, will try creating one', file = log)
                                    try:
                                        # create a string with the SQL Insert that uses bind variables to pass the variables. See here: https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html#bind
                                        insertSQL = "INSERT INTO PCAS_ExternalAccountMap (ApplicationUserType, OpenIDIssuerURL, OpenIDUserAccountID, PCAS_AccountToken, PCAS_ExternalAccountMapID) VALUES (:AccountType, :URL, :email, :studentToken, :entryNumber)"
                                        cur.execute(insertSQL, ["STUDENT", "https://accounts.google.com", email, studentIdentifier, newEntry]) # execute the insertion, pass the identity provider, email, their student identifier, and the count
                                        con.commit() # COMMIT THE CHANGES INTO THE DATABASE
                                        newEntry = newEntry + 1 # increment the new entry counter, probably not needed as it should get re-pulled each student but just in case
                                    except Exception as err:
                                        print(f'ERROR on insertion of new PCAS STUDENT account for user {email} with DCID {dcid}: {err}')
                                        print(f'ERROR on insertion of new PCAS STUDENT account for user {email} with DCID {dcid}: {err}', file=log)

                            else: # if they didnt have an access result, their old login info needs to be populated
                                print(f'ERROR on user {email} with DCID {dcid} when getting student identifier from AccessStudent table, needs to have old login info populated!')
                                print(f'ERROR on user {email} with DCID {dcid} when getting studnet identifier from AccessStudent table, needs to have old login info populated!', file=log)

                    except Exception as err: # general error catching
                        print(f'ERROR on user {email} at building {homeschool} with DCID: {dcid} | Error: {err}')
                        print(f'ERROR on user {email} at building {homeschool} with DCID: {dcid} | Error: {err}', file=log)

                endTime = datetime.now()
                endTime = endTime.strftime('%H:%M:%S')
                print(f'INFO: Execution ended at {endTime}')
                print(f'INFO: Execution ended at {endTime}', file=log)