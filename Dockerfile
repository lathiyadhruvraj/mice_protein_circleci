FROM python:3.7
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
# FROM python:3.7
# COPY . .
# WORKDIR /app
RUN mkdir -p /app/Prediction_Logs
RUN mkdir -p /app/Prediction_Custom_Files
RUN mkdir -p /app/Prediction_Batch_Files
RUN mkdir -p /app/Prediction_Database
RUN mkdir -p /app/Prediction_FileFromDB
RUN mkdir -p /app/Prediction_Output_File
RUN mkdir -p /app/Prediction_Raw_Files_Validated
RUN mkdir -p /app/Prediction_Raw_Files_Validated/Good_Raw
RUN mkdir -p /app/Prediction_Raw_Files_Validated/Bad_Raw
RUN mkdir -p /app/PredictionArchivedBadData

ENTRYPOINT [ "python" ]
CMD [ "app.py" ]



# ENTRYPOINT ["streamlit","run"]
# CMD ["app.py"]

