FROM python:3.7
COPY . /app
WORKDIR /app

RUN mkdir -p /Prediction_Logs
RUN mkdir -p /Prediction_Custom_Files
RUN mkdir -p /Prediction_Database
RUN mkdir -p /Prediction_FileFromDB
RUN mkdir -p /Prediction_Output_File
RUN mkdir -p /Prediction_Raw_Files_Validated
RUN mkdir -p /Prediction_Raw_Files_Validated/Good_Raw
RUN mkdir -p /Prediction_Raw_Files_Validated/Bad_Raw
RUN mkdir -p /PredictionArchivedBadData

RUN pip install -r requirements.txt
ENTRYPOINT [ "python" ]
CMD [ "app.py" ]