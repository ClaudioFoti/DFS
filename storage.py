from flask import Flask, request
from pathlib import Path
import os
import sqlite3

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = "files"

db = sqlite3.connect('storage.db', check_same_thread=False)

@app.route("/")
def index():
    return "home"

@app.route("/upload_file",methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'filename' not in request.form or 'content' not in request.form or 'uuid' not in request.form:
            return "Missing argument(s)", 400
        else:
            filename = request.form["filename"]
            content = request.form["content"]
            uuid = request.form["uuid"]
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

            with open(filepath,'w') as file:
                file.write(content)

            size = os.path.getsize(filepath)
            
            save_file_db(uuid,filename,filepath,content,size)

            return "File uploaded", 200
    return "Only POST requests allowed", 405

@app.route("/storing",methods=['GET'])
def get_current_storage_size():
    cursor = db.cursor()

    cursor.execute('SELECT SUM(size) FROM files;')

    size = cursor.fetchone()[0]

    if not (size is None):
        return str(size), 200
    else:
        return str(0), 200

@app.route("/files/<filename>")
def get_file(filename):
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    if os.path.isfile(filepath):
        return Path(filepath).read_text()
    else:
        return ""

def save_file_db(uuid,filename,location,content,size):
    cur = db.cursor()

    cur.execute("INSERT INTO files (uuid,filename,location,content,size) VALUES ('"+uuid+"','"+filename+"','"+location+"','"+content+"',"+str(size)+")")

    db.commit()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)