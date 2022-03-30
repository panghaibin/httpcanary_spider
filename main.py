import os
import time
import zipfile
from flask import Flask, request
from parse import ParseDir
from mtms import MTMSParser, MTMSComment

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'upload')
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def get_http_canary_file_name():
    str_time = time.strftime('%Y-%m-%d_%H-%M-%S', time.localtime(time.time()))
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], str_time + '.ZIP')
    return file_path


def unzip(zip_file):
    ext_path = zip_file[zip_file.rfind("\\") + 1:][:-4]
    # print(extPath)
    file = zipfile.ZipFile(zip_file, 'r')
    extract_path = os.path.join('.', app.config['UPLOAD_FOLDER'], ext_path)
    for f in file.namelist():
        file.extract(f, extract_path)
    file.close()
    return extract_path


def save_to_db(path):
    parse_dir_result = ParseDir(path)
    mtms = MTMSParser(parse_dir_result)
    mtms.save_all()
    mtms_comment = MTMSComment(parse_dir_result)
    mtms_comment.get_all()
    mtms_comment.save_all()


@app.route('/httpcanary', methods=['GET', 'POST'])
def httpcanary():
    if request.method == 'POST':
        # print(request.headers)
        zip_name = get_http_canary_file_name()
        with open(str(zip_name), "wb") as f2:
            f2.write(request.get_data())
        path = unzip(zip_name)
        print(path)
        save_to_db(path)

    return 'ok'


if __name__ == '__main__':
    app.debug = True
    app.run(host="0.0.0.0", port=5000)
