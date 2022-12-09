
from application_logging import logger
from data_ingestion import data_loader
from data_preprocessing import preprocessing
'''
in this class we we have define the tranining process  
'''




class trainmodel:
    def __init__(self):
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def traininigmodel(self):
        #logging to start for training
        self.log_writer.log(self.file_object, 'Start of Training')
        #try:
        # getting the data from the source
        data_getter = data_loader.Data_Getter(self.file_object,self.log_writer)
        data = data_getter.get_data()

        #doing the data preprocessing

        preprocessor = preprocessing.Preprocessor(self.file_object,self.log_writer)

        # create seprate features and labels
        X,Y=preprocessor.seprate_label_feature(data,label_column_name='default payment next month')

        # check if missing values are present in the dataset

        is_null_present,cols_with_missing_values=preprocessor.is_null_present(X)













