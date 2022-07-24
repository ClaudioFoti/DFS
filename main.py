from flask import Flask,request
from pathlib import Path
import requests
import os
import uuid
import sqlite3

SERVER_POOL = [("127.0.0.1",8081),("127.0.0.1",8082),("127.0.0.1",8083)]

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "uploads"

db = sqlite3.connect('main.db', check_same_thread=False)

@app.route("/")
def index():
    return str(shard_file())

@app.route('/create_file', methods=['GET', 'POST'])
def upload_file():
    response = ""
    status_code = 500
    if request.method == 'POST':
        if 'file' not in request.files:
            response += "No file part"
        else:
            file = request.files['file']
            status_code = upload_file(file)

        if status_code == 200:
            response += "File successfully created"



    return '''
    <!doctype html>
    <title>Upload your file</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''+response

def allowed_file(filename):
    allowed_extensions = ['txt']
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in allowed_extensions

def upload_file(file):
    if file.filename == '':
        return "No file uploaded"
    if file and allowed_file(file.filename):

        filename = file.filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        file.save(filepath)

        content = Path(filepath).read_text()

        file_uuid = uuid.uuid4()

        request = requests.post("http://127.0.0.1:8081/upload_file", data={'filename': filename, 'content': content, 'uuid': file_uuid})

        return request.status_code
        
    return "File couldn't be uploaded"

def shard_file():
    sizes = []
    
    server_pool = SERVER_POOL

    for server in server_pool:
        url = "http://"+server[0]+":"+str(server[1])+"/storing"
        try:
            response = requests.get(url,timeout=1)
            status_code = response.status_code
        except:
            status_code = 503
        if status_code == 200:
            sizes.append(("http://"+server[0]+":"+str(server[1]),int(response.text)))
    
    return sizes

def save_file_db(uuid,locations):
    cur = db.cursor()

    cur.execute("INSERT INTO files (uuid,locations) VALUES ('"+uuid+"','"+locations+"'")

    db.commit()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)