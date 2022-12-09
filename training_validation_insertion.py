from Training_raw_data_validation.raw_data_validation import raw_data_validation
from DataTransform_Training.DataTransformation import datatransform
from application_logging.logger import App_Logger
from DataTypeValidation_Insertion_Training.DataTypeValidation import dboperation
import os

'''
in this section we do is
1) raw data valiadation
   a)extracting details from schema file.
   b)check the manual regex format
   c)check the column length
   d)valiadtion of all missing value in any columns
2)data transformation
3)using database
'''


class train_validation:
    def __init__(self,path):

        self.raw_data = raw_data_validation(path)
        self.logger = App_Logger()
        self.cwd = os.getcwd()
        self.file_object = open(self.cwd+'Training_Main_Log.txt', 'a+')
        self.datatransform = datatransform()
        self.dboperation = dboperation()




    def train_validation(self):
        try:
            self.logger.log(self.file_object,"Start of Validation on files for Training")
            #extracting values from schema file
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesfromscema()
            #getting the regex to validate the name
            regex = self.raw_data.manualregexcreation()
            #validating file name of prediction files
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            #validating column length in file
            self.raw_data.checking_column_length(noofcolumns)
            #validating if any column has all missing value
            self.raw_data.column_all_missing()
            self.logger.log(self.file_object,"Raw Data Validation Complete!!")
            self.logger.log(self.file_object,"Starting Data Transforamtion!!")
            #replacing the empty value with null value
            self.datatransform.replacemissingwithnulls()
            self.logger.log(self.file_object, "DataTransformation Completed!!!")
            self.logger.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            self.dboperation.createtabledb("Training",column_names)
            self.logger.log(self.file_object, "Insertion in Table completed!!!")
            self.logger.log(self.file_object, "Deleting Good Data Folder!!!")
            #import csv files into the table
            self.dboperation.insertintotablegooddata("Training")
            self.logger.log(self.file_object,"Insertion in Table completed!!!")
            self.logger.log(self.file_object, "Deleting Good Data Folder!!!")
            #delete the good data folder after loading files in database
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.logger.log(self.file_object,"deleting good data folder completed")
            self.logger.log(self.file_object,"Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.movebadfilestoarchive()
            self.logger.log(self.file_object,"badfiles move to archive!! bad files deleted!!")
            self.logger.log(self.file_object,"validation operation completed")
            self.logger.log(self.file_object,"extracting csv file from table")
            #export csv file from db
            self.dboperation.selectingdatafromtablesasscv("Training")
            self.file_object.close()

        except Exception as e:
            raise e








