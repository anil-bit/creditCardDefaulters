import pandas as pd

class Data_Getter:
    def __init__(self,file_object,logger_object):
        self.training_file = "Training_FileFromDB/InputFile.csv"
        self.logger_object = logger_object
        self.file_object=file_object

    def get_data(self):
        '''
        method name:  get_data
        purpose: this method reads the data from the file
        '''
        self.logger_object.log(self.file_object,'Entered the get_data method of the Data_Getter class')
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger_object.log(self.file_object,"data loaded suceessfully and has extracted")
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,"error while loading the data, the expected error is:"+str(e))
            self.logger_object.log(self.file_object,"data load was unsucessfull.exited the get_data")
            raise Exception()



