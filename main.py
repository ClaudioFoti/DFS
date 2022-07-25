from flask import Flask,request,redirect
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
    html_list = ""
    for file in get_files():
        filename = file["filename"]
        file_uuid = file["uuid"]
        html_list += "<li><a href=\"/read_file/"+file_uuid+"\">"+filename+"</a></li>"
    
    return '''
    <!doctype html>
    <title>All files</title>
    <h1>All files</h1>
    <ul>'''+html_list+"</ul>"+'''
    <br>
    <a href=\"/create_file\">Upload a file</a>
    '''

@app.route('/read_file/<file_uuid>')
def read_file(file_uuid):
    content = get_file_content(get_file_locations(file_uuid))
    
    return '''
    <!doctype html>
    <title>File #'''+file_uuid+'''</title>
    <h1>File #'''+file_uuid+'''</h1>
    <form method=post action="/edit_file">
        <p><label for="content">File content:</label></p>
        <input name="file_uuid" type="hidden" value='''+file_uuid+'''></input>
        <textarea id="content" name="content" rows="20" cols="100">'''+content+"</textarea>"+'''
        <br>
        <input type=submit value=Edit>
    </form>
    <br>
    <a href=\"/delete/'''+file_uuid+'''\">Delete file</a>
    <br>
    <a href=\"/\">Go to file list</a>
    '''

@app.route('/create_file', methods=['GET', 'POST'])
def upload_file():
    response = ""
    if request.method == 'POST':
        if 'file' not in request.files:
            response += "No file part"
        else:
            file = request.files['file']
            response += upload_file(file)

    return '''
    <!doctype html>
    <title>Upload your file</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''+response+'''
    <br>
    <a href=\"/\">Go to file list</a>
    '''

@app.route('/delete/<file_uuid>')
def delete(file_uuid):
    for location in get_file_locations(file_uuid):
        requests.get("http://"+location.split('/')[2]+"/delete/"+file_uuid)
    
    cur = db.cursor()

    cur.execute('DELETE FROM files WHERE uuid = "'+file_uuid+'";')

    db.commit()

    response = "File successfully deleted"
    return '''
    <!doctype html>
    <title>Delete file</title>
    <h1>Delete file</h1>
    '''+response+'''
    <br>
    <a href=\"/\">Go to file list</a>
    '''

@app.route('/edit_file', methods=['POST'])
def edit_file():
    response = ""
    if request.method == 'POST':
        if 'content' not in request.form:
            response += "No content submitted"
        else:
            file_uuid = request.form['file_uuid']
            content = request.form['content']
            filename = get_file_name(file_uuid)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            delete(file_uuid)

            with open(filepath,'w') as file:
                file.write(content)

            response += shard_file(file_uuid,filename,filepath,content)

            return redirect("/", code=302)
    return response


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

        return shard_file(file_uuid, filename, filepath, content)
        
    return "File couldn't be uploaded"

def get_servers_storage():
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
            sizes.append({"address": "http://"+server[0]+":"+str(server[1]),"size": int(response.text)})
    
    sorted_sizes = sorted(sizes, key=lambda d: d['size'])

    return sorted_sizes

def shard_file(file_uuid, filename, filepath, content):
    response = ""

    file_size_limit = 1000
    
    servers_storage = get_servers_storage()

    i = 0
    bytes = None
    with open(filepath, "r", encoding="utf8") as in_file:
        bytes = in_file.read(file_size_limit)
        while bytes:
            with open(os.path.join(app.config['UPLOAD_FOLDER'], (str(file_uuid) + "_" + str(i))), 'w', encoding="utf8") as output:
                output.write(bytes)
            bytes = in_file.read(file_size_limit)
            i += 1

    if i == 1:
        new_file_name = (str(file_uuid) + "_" + str(0))
        store_file(servers_storage[0]["address"],file_uuid, filename,new_file_name, filepath, content)

        response += "File successfully uploaded"

        os.remove(os.path.join(app.config['UPLOAD_FOLDER'], (str(file_uuid) + "_" + str(0))))
    else:
        for shard in range(i):
            new_file_name = (str(file_uuid) + "_" + str(shard))
            new_file_path = os.path.join(app.config['UPLOAD_FOLDER'], new_file_name)

            content = Path(new_file_path).read_text()

            servers_storage[0]["size"] += os.path.getsize(new_file_path)

            store_file(servers_storage[0]["address"],file_uuid, filename,new_file_name,new_file_path,content)
            
            servers_storage = sorted(servers_storage, key=lambda d: d['size'])

        response += "File successfully sharded"

        os.remove(filepath)


    return response

def store_file(address, file_uuid, original_filename, filename, filepath, content):
    requests.post(address+"/upload_file", data={'filename': filename, 'content': content, 'uuid': file_uuid})

    save_file_db(file_uuid,original_filename,address+"/files/"+filename)
    
    os.remove(filepath)

def save_file_db(file_uuid, filename,location):
    cur = db.cursor()

    cur.execute("INSERT INTO files (filename,uuid,location) VALUES ('"+filename+"','"+str(file_uuid)+"','"+location+"')")

    db.commit()

def get_files():
    files = []

    cursor = db.cursor()

    file_id = -1
    for row in cursor.execute('SELECT * FROM files ORDER BY id'):
        if not any(d['uuid'] == row[2] for d in files):
            files.append({"filename": row[1], "uuid": row[2],"locations": [row[3]]})
            file_id += 1
        else:
            files[file_id]["locations"].append(row[3])

    return files

def get_file_locations(file_uuid):
    locations = []

    cursor = db.cursor()

    for row in cursor.execute('SELECT location FROM files WHERE uuid = "' + file_uuid + '";'):
        locations.append(row[0])

    return locations

def get_file_name(file_uuid):
    cursor = db.cursor()

    cursor.execute('SELECT filename FROM files WHERE uuid = "' + file_uuid + '" LIMIT 1;')

    filename = cursor.fetchone()[0]

    return filename

def get_file_content(locations):
    content = ""

    for location in locations:
        response = requests.get(location)
        if response.status_code == 200:
            content += response.text

    return content

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)