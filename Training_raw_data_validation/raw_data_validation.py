import json
import os.path

import pandas as pd

from application_logging.logger import App_Logger
from os import listdir
import re
import shutil

class raw_data_validation:
    def __init__(self,path):
        self.schema_path = "schema_training.json"
        self.Batch_Directory = path
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
                f.close()
                pattern  = dic['SampleFileName']
                LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
                LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
                NumberofColumns = dic["NumberofColumns"]
                column_names = dic['ColName']
                file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
                self.logger.log(file,message)
                file.close()

                #print("anil")


        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualregexcreation(self):

            '''
            method name = manularegexcreation
            purpose: this meyjod is used to check the file name format it should be equal to shema file provided
            output?: regex pattern
            '''
            regex = "['creditCardFraud']+['\_'']+[\d_]+[\d]+\.csv"
            return regex


    def deleteExistingBadDataTrainingFolder(self):
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


    def deleteExistingGoodDataTrainingFolder(self):

        '''
        methodname: delete_exsisting_gooddata_training_folder
        purpose:this method deletes the gooddata after the data is stored in database
        '''
        try:
            path = "Training_Raw_files_validated/"
            if os.path.isdir(path + "Good_Raw/"):
                shutil.rmtree(path + "Good_Raw/")
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError






    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """


        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        #create new directories
        self.createDirectoryForGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """

        try:
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def checking_column_length(self,NumberofColumns):
        '''
        method: checking_column_length
        purpose: it validates the no of columns in file that should be equal to no of files in schema file

        '''
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            #print(self.Batch_Directory)
            #file = [f for f in listdir(self.Batch_Directory)]
            #print(file)
            for file in listdir(self.Batch_Directory):
                data = pd.read_csv(self.Batch_Directory + "/" + file)
                #print(data)
                #print(data.shape[1])
                #print(NumberofColumns)
                if data.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f,"column length validation is completed")

        except OSError:
            f = open("Training_Logs/columnValidationLog.txt","a+")
            self.logger.log(f,"Error Occured while moving the file:: %s" % OSError)
            f.close()
            raise OSError

        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt","a+")
            self.logger.log(f,"Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def column_all_missing(self):
        '''

        method:column_all_missing
        description:remove the column that has all missing value in the column
        '''

        #print(self.Batch_Directory)
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"missing values validation started")
            for file in listdir(self.Batch_Directory):
                print(file)
                data = pd.read_csv(self.Batch_Directory + "/" + file)
                #print(data.columns)
                #print(len(data.columns))
                #print(data["LIMIT_BAL"].isnull().sum())
                for col in data.columns:
                    if len(data[col]) == data[col].isnull().sum():
                        #print("not ok")
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Invalid Column for the file!! File moved to Bad Raw Folder :: %s" % file)
                        print("not ok")
                        break

        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()




















