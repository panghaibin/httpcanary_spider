import os
import time
import zipfile
from flask import Flask, request
from parse import ParseDir
from mtms import MTMSParser

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'upload')
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def GetHttpCanaryFileName():
    str_time = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(time.time()))
    FilePath = os.path.join(app.config['UPLOAD_FOLDER'], str_time + '.ZIP')
    return FilePath


def unZip(ZipFile):
    extPath = ZipFile[ZipFile.rfind("\\") + 1:][:-4]
    # print(extPath)
    file = zipfile.ZipFile(ZipFile, 'r')
    extract_path = os.path.join('.', app.config['UPLOAD_FOLDER'], extPath)
    for f in file.namelist():
        file.extract(f, extract_path)
    file.close()
    return extract_path


def save_to_db(path):
    parse_dir_result = ParseDir(path)
    mtms = MTMSParser(parse_dir_result)
    mtms.save_all()


@app.route('/httpcanary', methods=['GET', 'POST'])
def httpcanary():
    if request.method == 'POST':
        print(request.headers)
        Zip = GetHttpCanaryFileName()
        with open(str(Zip), "wb") as f2:
            f2.write(request.get_data())
        path = unZip(Zip)
        print(path)
        # save_to_db(path)

    return 'ok'


if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=5000)
