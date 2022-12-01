from Training_raw_data_validation.raw_data_validation import raw_data_validation
from application_logging.logger import App_Logger
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
    def __init__(self):
        self.raw_data = raw_data_validation()
        self.logger = App_Logger()
        self.cwd = os.getcwd()
        self.file_object = open(self.cwd+'Training_Main_Log.txt', 'a+')



    def train_validation(self):
        self.logger.log(self.file_object,"Start of Validation on files for Training")
        #extracting values from schema file
        LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesfromscema()
        #getting the regex to validate the name
        regex = self.raw_data.manualregexcreation()
        #validating file name of prediction file




