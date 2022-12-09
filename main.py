
from training_validation_insertion import train_validation
from training_model import trainmodel
path = "/home/anil/Videos/my_project/creditCardDefaulters/scratch/Training_Batch_Files"
train_valObj = train_validation(path)
train_valObj.train_validation()
trainModelobj = trainmodel()
trainModelobj.traininigmodel()

