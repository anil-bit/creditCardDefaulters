import csv
import os.path
import sqlite3
from application_logging.logger import App_Logger
from os import listdir
import shutil
class dboperation:
    def __init__(self):
        self.path = "Training_Database/"
        self.logger = App_Logger()
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
    def databaseconnection(self,DatabaseName):
        '''
        method:databaseconnection
        purpose:to connect the data base

        '''
        try:
            conn = sqlite3.connect(self.path+DatabaseName+".db")
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % DatabaseName)
            file.close()
        except ConnectionError:
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def createtabledb(self,DatabaseName,column_names):
        '''
        method: create table
        purpose:create the table
        '''
        try:
            conn = self.databaseconnection(DatabaseName)
            c = conn.cursor()
            c.execute("SELECT count(name)  FROM sqlite_master WHERE type = 'table'AND name = 'Good_Raw_Data'")
            if c.fetchone()[0]==1:
                conn.close()
                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()

            else:

                for key in column_names.keys():
                    type = column_names[key]

                    #in try block we check if the table exists, if yes then add columns to the table
                    # else in catch block we will create the table
                    try:
                        #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                        conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))


                    # try:
                    #     #cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Bad_Raw_Data'".format(dbName=DatabaseName))
                    #     conn.execute('ALTER TABLE Bad_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                    #
                    # except:
                    #     conn.execute('CREATE TABLE Bad_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))


                conn.close()

                file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % DatabaseName)
                file.close()

        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            file.close()
            raise e

    def insertintotablegooddata(self,Database):
        '''
        method:insertintotablegooddata
        purpose:insert the data into database table
        '''
        conn = self.databaseconnection(Database)
        onlyfiles = [f for f in listdir(self.goodFilePath)]
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(self.goodFilePath+"/"+file,"r") as f:
                    next(f)
                    reader = csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in line[1]:
                            try:
                                conn.execute("INSERT INTO Good_Raw_Data values ({values})".format(values=(list_)))
                                self.logger.log(log_file," %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e

            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s " % e)
                shutil.move(self.goodFilePath+'/' + file, self.badFilePath)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()

        conn.close()
        log_file.close()

    def selectingdatafromtablesasscv(self,Database):
        '''
        method:  selectingdatafromtablesasscv
        purpose: certe a single table that consistes of all the data in database and save it in local
        '''
        self.filefromdb = 'Training_FileFromDB/'
        self.filename = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        try:
            conn = self.databaseconnection(Database)
            sqlselect = "SELECT * FROM Good_Raw_Data"
            cursor = conn.cursor()
            cursor.execute(sqlselect)
            results = cursor.fetchall()
            #print(cursor.description)
            headers = [i[0] for i in cursor.description]
            if not os.path.isdir(self.filefromdb):
                os.makedirs(self.filefromdb)
            csvFile = csv.writer(open(self.filefromdb + self.filename, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            #add headers and data into csv files
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File exporting failed. Error : %s" %e)
            log_file.close()






