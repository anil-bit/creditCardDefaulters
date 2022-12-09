import pandas as pd
import numpy as np

class Preprocessor:
    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object


    def seprate_label_feature(self,data,label_column_name):
        '''
        method: seprate_label_feature
        purpose:seperate x and y for training the label and features
        '''
        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.x_data=data.drop(labels = label_column_name,axis = 1)# drop the label feature from data
            self.y_data = data[label_column_name]# select the
            self.logger_object.log(self.file_object,"Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class")
            return self.x_data,self.y_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()

    def is_null_present(self,data):
        '''
        method: is_null_present
        purpose: check wether null value is present or not
        '''
        self.null_present = False
        self.cols_with_missing_values = []

        try:
            self.null_counts= data.isna().sum()# check the values
            for i in range(len(self.null_counts)):
                self.null_counts[i] > 0
                self.null_present = True
                self.cols_with_missing_values.append(self.null_counts[i])
            if(self.null_present): # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null["columns"] = data.columns
                self.dataframe_with_null["missing value count"] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv') # storing the null column information to file
            self.logger_object.log(self.file_object,'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise Exception()

