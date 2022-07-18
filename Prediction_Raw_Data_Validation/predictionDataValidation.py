# import sqlite3
from datetime import datetime
from os import listdir
import os, sys
import re
import json
import shutil
import pandas as pd
from Logging_Layer.logger import app_logger
from Exception import HousingException


class Prediction_Data_validation:

    def __init__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_prediction.json'
        self.logger = app_logger()

    def valuesFromSchema(self):

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise HousingException(e,sys) from e

        except KeyError as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise HousingException(e,sys) from e

        except Exception as e:
            file = open("Prediction_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise HousingException(e,sys) from e 

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):

        regex = "['mice_protein']+['\_'']+[\d_]+[\d]+\.xls"
        return regex

    def createDirectoryForGoodBadRawData(self):

        try:
            path = os.path.join("Prediction_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Prediction_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % e)
            file.close()
            raise HousingException(e,sys) from e 

        except Exception as e:
            raise HousingException(e,sys) from e

    def deleteExistingGoodDataTrainingFolder(self):

        try:
            path = 'Prediction_Raw_Files_Validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % e)
            file.close()
            raise HousingException(e,sys) from e
        except Exception as e:
            raise HousingException(e,sys) from e 

    def deleteExistingBadDataTrainingFolder(self):

        try:
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Prediction_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % e)
            file.close()
            raise HousingException(e,sys) from e 
        except Exception as e:
            raise HousingException(e,sys) from e 

    def moveBadFilesToArchiveBad(self):

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            path = "PredictionArchivedBadData"
            if not os.path.isdir(path):
                os.makedirs(path)
            source = 'Prediction_Raw_Files_Validated/Bad_Raw/'
            dest = 'PredictionArchivedBadData/BadData_' + str(date) + "_" + str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files = os.listdir(source)
            for f in files:
                if f not in os.listdir(dest):
                    shutil.move(source + f, dest)
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Bad files moved to archive")
            path = 'Prediction_Raw_Files_Validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
            self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
            file.close()
        except OSError as e:
            file = open("Prediction_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise HousingException(e,sys) from e 
        except Exception as e:
            raise HousingException(e,sys) from e 

    def validationFileNameRaw(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):

        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()
        only_files = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            for filename in only_files:
                if re.match(regex, filename):
                    splitAtDot = re.split('.xls', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[2]) == LengthOfDateStampInFile:
                        if len(splitAtDot[3]) == LengthOfTimeStampInFile:
                            shutil.copy(os.path.join(self.Batch_Directory, filename) , "Prediction_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy(os.path.join(self.Batch_Directory, filename), "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("Prediction_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occurred while validating FileName %s" % e)
            f.close()
            raise HousingException(e,sys) from e 

    def validateColumnLength(self, NumberofColumns):

        try:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            
            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                csv = pd.read_excel("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                else:
                    shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                "Prediction_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)

            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise HousingException(e,sys) from e
        except Exception as e:
            f = open("Prediction_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occurred:: %s" % e)
            f.close()
            raise HousingException(e,sys) from e 

        f.close()


    def deletePredictedFiles(self):
        # if os.path.exists('Prediction_Output_File/Predictions.csv'):
        #     os.remove('Prediction_Output_File/Predictions.csv')

        dir_Prediction_Output_File = os.listdir("Prediction_Output_File")

        filtered_Output_files = [file for file in dir_Prediction_Output_File if file.endswith(".csv")]
        for file in filtered_Output_files:
            path_to_file = os.path.join("Prediction_Output_File", file)
            os.remove(path_to_file)

        dir_Prediction_FileFromDB = os.listdir("Prediction_FileFromDB")

        filtered_filesFromDB = [file for file in dir_Prediction_FileFromDB if file.endswith(".csv")]
        for file in filtered_filesFromDB:
            path_to_file = os.path.join("Prediction_FileFromDB", file)
            os.remove(path_to_file)

        dir_plots = os.listdir("plots")

        filtered_filesFromDB = [file for file in dir_plots if file.endswith(".png")]
        for file in filtered_filesFromDB:
            path_to_file = os.path.join("plots", file)
            os.remove(path_to_file)


    def validateMissingValuesInWholeColumn(self):

        try:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Prediction_Raw_Files_Validated/Good_Raw/'):
                # xls = pd.ExcelFile("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                # csv = xls.parse(0)
                csv = pd.read_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("Prediction_Raw_Files_Validated/Good_Raw/" + file,
                                    "Prediction_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder"
                                           " :: %s" % file)
                        break
                if count == 0:
                    csv.to_csv("Prediction_Raw_Files_Validated/Good_Raw/" + file, index=None, header=True)
                    self.logger.log(f, "Everything OK! File moved to Good_Raw")

        except OSError as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred while moving the file :: %s" % OSError)
            f.close()
            raise HousingException(e,sys) from e 
        except Exception as e:
            f = open("Prediction_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occurred:: %s" % e)
            f.close()
            raise HousingException(e,sys) from e 
        f.close()
