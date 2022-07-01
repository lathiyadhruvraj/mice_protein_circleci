from flask.helpers import send_file
from werkzeug.utils import secure_filename
from flask import Flask, request, render_template, send_file, flash
from flask import Response
import shutil
import os
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction
from trainingModel import TrainModel
from training_validation_insertion import TrainValidation
from wsgiref.simple_server import make_server
from flask_cors import CORS, cross_origin


application = Flask(__name__)
app = application

cors = CORS(app)

app.config['SECRET_KEY'] = "thisissecretkey"


predict = 0
upload = 0

@cross_origin()
@app.route("/", methods=['GET'])
def home():
    return render_template('index.html')

@cross_origin()
@app.route("/predict", methods=['POST'])
def predictRouteClient():
    global predict, upload
    try:
        # if request.json is not None:
        #     path = request.json['filepath']

        #     pred_val = pred_validation(path)  # object initialization

        #     pred_val.prediction_validation()  # calling the prediction_validation function

        #     pred = prediction(path)  # object initialization

        #     path = pred.predictionFromModel() # predicting for dataset present in database

        #     return Response("Prediction File created at %s!!!" % path)

        if request.method == 'POST':

            if (request.form.get('button') == "Predict My Files") and (upload == 1):
                path = "Prediction_Custom_Files"
                print('custom call')
            elif request.form.get('button') == "Predict Default Files":
                flash("DATASTAX ASTRA Hibernates the database if not used with in 24 hrs.", "info")
                flash("Contact 7383857575 to activate & see it working", "info")
                path = "Prediction_Batch_Files"
                print('default call')
            
            predict, upload = 1, 0
            
            pred_val = pred_validation(path)  # object initialization

            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path)  # object initialization

            pred.predictionFromModel()  # predicting for dataset present in database

            flash("Prediction files are ready! You can download !", "success")
            return render_template('index.html')
                
        flash("UPLOAD FILES TO PREDICT OR CHOOSE DEFAULT FILES OPTION", "info")
        return render_template('index.html')

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)
    except KeyError:
        return Response("Error Occurred! %s" % KeyError)
    except Exception as e:
        predict = 0
        flash(f"Caught Exception: {e}", "warn")
        flash("Make sure you are selecting atleast 1 file with proper .xls Format AND Click Upload button", "info")
        return render_template('index.html')

@cross_origin()
@app.route("/train", methods=['POST'])
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']
            # path = "Training_Batch_Files"
            # path = json.loads(path_str)

            train_valObj = TrainValidation(path)
            train_valObj.train_validation()

            trainModelObj = TrainModel()
            trainModelObj.trainingModel()

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)

    except KeyError:
        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:
        return Response("Error Occurred! %s" % e)

    return Response("Training successful!!")

@cross_origin()
@app.route("/upload_custom_files", methods=["POST"])
def upload_files():
    try:
        if request.method == "POST":
            global predict, upload
            upload = 1

            files_list = request.files.getlist("file")            
            
            file_saved = []
            file_not_saved = []
            file_saved.clear()
            file_not_saved.clear()

            for file in files_list:
                if file.filename.split(".")[1] == "xls":
                    file.save(os.path.join(os.getcwd(), "Prediction_Custom_Files", secure_filename(file.filename)))
                    flash(str(file.filename) + " File uploaded", "success")
                    file_saved.append(file.filename)
                else:
                    file_not_saved.append(file.filename)
                    flash("File Uploaded is not in .xls Format", "warn")


            if len(file_saved) > 0:
                flash("To Predict Press PREDICT MY FILES and wait until process gets completed.", "info")
                flash("DO NOT REFRESH/DO NOT PRESS BUTTONS while YOUR FILE/S IN PROCESS", "warn")
                if len(file_not_saved) > 0:
                    message = ' '.join(map(str, file_not_saved)) +  "\n File/s Rejected. Invalid file extension. Only (.xls) allowed"
                    flash(message, "error")
                    return render_template('index.html')
                
                return render_template('index.html')
        
        flash("Upload proper file/s", "info")
        return render_template('index.html')

    except Exception as e:
        flash(f"Caught Exception = {e}", "error")
        flash(" File Not Uploaded. Make sure you are selecting a file", "error")
        return render_template('index.html')

@cross_origin()
@app.route("/download", methods=['POST'])
def download_file():
    global predict
    try:
        if predict:
            predict = 0
            
            shutil.make_archive("Prediction_Files", "zip", "Prediction_Output_File")


            return send_file('Prediction_Files.zip',
                    mimetype = 'zip',
                    attachment_filename= 'Prediction_Files.zip',
                    as_attachment = True)
           
        else:
            head = "PREDICTION FILE NOT AVAILABLE," \
                   " UPLOAD YOUR FILE THEN PRESS PREDICT MY FILE OR PRESS PREDICT AVAILABLE FILE"
            flash(head, "error")
            return render_template('index.html')

    except Exception as e:
        flash(e, "error")
        return render_template('index.html')

port = int(os.getenv('PORT', 5000))
if __name__ == "__main__":
    host= '0.0.0.0'
    httpd = make_server(host=host, port=port, app=app)
    httpd.serve_forever()
    # app.run(debug=True, host=host, port=port)
    