import os
import re
import json
import socket
import urllib3
from io import BytesIO
from urllib.parse import urlparse
from http.client import HTTPResponse
from http.server import BaseHTTPRequestHandler


class BytesIOSocket(socket.socket):
    def __init__(self, content):
        super().__init__()
        self.handle = BytesIO(content)

    def makefile(self, *args, **kwargs):
        return self.handle


class HTTPRequest(BaseHTTPRequestHandler):
    def __init__(self, data):
        self.rfile = BytesIO(data)
        self.raw_requestline = self.rfile.readline()
        self.error_code = self.error_message = None
        self.parse_request()

    def send_error(self, code, message=None, explain=None):
        self.error_code = code
        self.error_message = message


class PhaseResponse:
    def __init__(self, file_path):
        with open(file_path, 'rb') as f:
            self.file_content = f.read()
        self.response = self._response_from_bytes(self.file_content)

    @staticmethod
    def _response_from_bytes(data):
        sock = BytesIOSocket(data)
        response = HTTPResponse(sock)
        response.begin()
        return urllib3.HTTPResponse.from_httplib(response)

    def get_file_content(self):
        return self.file_content

    def get_response_data(self):
        return self.response

    def get_json(self):
        return json.loads(self.response.data.decode('utf-8'))

    def get_url_phase(self):
        return urlparse(self.response.data)

    def print_json(self):
        print(json.dumps(self.get_json(), indent=4, ensure_ascii=False))


class PhaseRequest:
    def __init__(self, file_path):
        with open(file_path, 'rb') as f:
            self.file_content = f.read()
        self.request = self._request_from_bytes(self.file_content)

    @staticmethod
    def _request_from_bytes(data):
        return HTTPRequest(data)

    def check_err(self):
        if self.request.error_code is not None:
            return False
        return True

    def get_file_content(self):
        return self.file_content

    def get_request_data(self):
        return self.request

    def get_host(self):
        return self.request.headers['host']

    def check_host(self, regx):
        if re.match(regx, self.get_host()):
            return True
        return False


class PhaseFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.req_path = None
        self.res_path = None
        self.type = self.check_type()
        self._phase_file_path()
        self.request = PhaseRequest(self.req_path)
        self.response = PhaseResponse(self.res_path)

    def check_type(self):
        if self.file_path.startswith('http_req'):
            return 'req'
        elif self.file_path.startswith('http_res'):
            return 'res'
        else:
            return 'unknown'

    def _phase_file_path(self):
        if self.type == 'req':
            self.req_path = self.file_path
            self.res_path = self.file_path.replace('http_req', 'http_res')
        elif self.type == 'res':
            self.req_path = self.file_path.replace('http_res', 'http_req')
            self.res_path = self.file_path


class PhaseDir:
    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.files = self.get_files()
        self.phase_files = self.get_phase_files()

    def get_files(self):
        files = []
        for file in os.listdir(self.dir_path):
            if file.startswith('http_req'):
                files.append(file)
        return files

    def get_phase_files(self):
        phase_files = []
        for file in self.files:
            phase_files.append(PhaseFile(os.path.join(self.dir_path, file)))
        return phase_files

    def filter_req_host(self, regx):
        phase_files = []
        for phase_file in self.phase_files:
            if phase_file.request.check_host(regx):
                phase_files.append(phase_file)
        return phase_files


if __name__ == '__main__':
    file_name = 'rawhttp_res_8b2b421b-55a9-406f-80d8-891f76d854dc.hcy'
    phase_response = PhaseResponse(file_name)
    phase_response.print_json()

    req_name = 'http_req_8b2b421b-55a9-406f-80d8-891f76d854dc.hcy'
    phase_request = PhaseRequest(req_name)
    print(phase_request.check_host(r'mapi.dianping.com'))
    UPLOAD_FOLDER = os.path.join(os.getcwd(), 'upload')  # 上传路径
    print(UPLOAD_FOLDER)
