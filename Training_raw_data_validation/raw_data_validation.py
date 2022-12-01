import json
import os.path

from application_logging.logger import App_Logger
from os import listdir
import re
import shutil

class raw_data_validation:
    def __init__(self,path):
        self.schema_path = "schema_training.json"
        self.batch_dir = path
        self.logger = App_Logger()

    def valuesfromscema(self):
        '''
        method name = valuesfromscema
        purpose: to extract the information providef by the schema file
        input: the schema file
        output: LengthOfDateStampInFile,
                LengthOfTimeStampInFile,
                column_names,
                Number of Columns.



        '''
        #open json file which is scema file of trainig data
        try:
            with open(self.schema_path,"r") as f:
                dic = json.load(f)
                #f.close()
                samplefilename = dic['SampleFileName']
                lengthofdatestample = dic["LengthOfDateStampInFile"]
                lengthoftimestampfile = dic["LengthOfTimeStampInFile"]
                numberofcolumns = dic["NumberofColumns"]
                file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                message = "SampleFileName:: %s"%samplefilename + "\t" + "LengthOfDateStampInFile:: %s"%lengthofdatestample + "\t" + "LengthOfTimeStampInFile:: %s"%lengthoftimestampfile + "\t" + "NumberofColumns:: %s"%numberofcolumns + "\n"
                self.logger.log(file,message)
                file.close()
                return samplefilename,lengthofdatestample,lengthoftimestampfile,numberofcolumns
                #print("anil")

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

    def manualregexcreation(self):

            '''
            method name = manularegexcreation
            purpose: this meyjod is used to check the file name format it should be equal to shema file provided
            output?: regex pattern
            '''
            regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
            return regex


    def delete_exsisting_baddata_trainig_folder(self):
        '''
        method name: delete_exsisting_baddata_trainig_folder
        description:delets the exsisting bad data in folder
        '''

        try:
            path = "Training_Raw_files_validated/"
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + "Bad_Raw/")
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()

        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError






    def validationfilenameeaw(self,regex,lengthofdatestample,lengthoftimestampfile):
        self.delete_exsisting_baddata_trainig_folder()
        self.delete_exsisting_gooddata_training_folder()
        onlyfiles = [f for f in listdir(self.batch_dir)]
        #print(onlyfiles)
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for files in onlyfiles:
                if (re.match(regex,files)):
                    splittatdot = re.split(".csv",files)
                    splittatdot = re.split("_",splittatdot[0])
                    if splittatdot[1]==lengthofdatestample:
                        if splittatdot[2]==lengthoftimestampfile:
                            shutil.copy("Training_Batch_Files/" + files, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % files)

                        else:
                            shutil.copy("Training_Batch_Files/" + files, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % files)
                    else:
                        shutil.copy("Training_Batch_Files/" + files, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % files)
                else:
                    shutil.copy("Training_Batch_Files/" + files, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % files)

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e








