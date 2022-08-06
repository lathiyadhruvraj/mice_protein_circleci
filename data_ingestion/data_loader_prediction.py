import pandas as pd
import os, sys
import csv
from Exception import HousingException

class Data_Getter_Pred:

    def __init__(self, file_object, logger_object, file):
        self.prediction_file = 'Prediction_FileFromDB'
        self.file_object = file_object
        self.logger_object = logger_object
        self.file = file

    def get_data(self):
        self.logger_object.log(self.file_object, 'Entered the get_data method of the Data_Getter class')
        file_path = os.path.join(os.getcwd(), self.prediction_file, self.file)
        try:
            if os.path.isfile(file_path):
                # file_path.seek(0)
                data = pd.read_csv(file_path)  # reading the data file
             
                # with open(file_path, "r") as csvfile:
                #     reader_variable = csv.reader(csvfile, delimiter=",")
                #     data = []
                #     data.clear()
                #     for row in reader_variable:
                #         # print(row)
                #         data.append(row)
                # data = pd.DataFrame(data)
                # data.rename(columns=data.iloc[0], inplace=True)
                # data.drop(data.index[0],  inplace=True)
                # print(type(data))
                # print(data.head(5))

                self.logger_object.log(self.file_object,
                                    'Data Load Successful.Exited the get_data method of the Data_Getter class')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occurred in get_data method of the Data_Getter class.'
                                                     ' Exception message: ' + str(e))
            self.logger_object.log(self.file_object,
                                   'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise HousingException(e,sys) from e  

