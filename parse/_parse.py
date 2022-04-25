import os
import re
import json
import socket
import urllib3
import logging
import http.client
from io import BytesIO
from urllib.parse import urlparse
from urllib.parse import parse_qs
from http.client import HTTPResponse
from http.server import BaseHTTPRequestHandler
http.client._MAXLINE = 655360


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


class ParseResponse:
    def __init__(self, file_path):
        self.file_path = file_path
        # logging.debug('[+] Reading file: {}'.format(file_path))
        self.file_content = self.get_file_content()
        self.response = self._response_from_bytes(self.file_content)
        self.data = self.response.data
        self.json = self.get_json()
        self.urlparse_query = self.get_urlparse_query()

    @staticmethod
    def _response_from_bytes(data):
        sock = BytesIOSocket(data)
        response = HTTPResponse(sock)
        response.begin()
        return urllib3.HTTPResponse.from_httplib(response)

    def get_file_content(self):
        with open(self.file_path, 'rb') as f:
            file_content = f.read()
        if file_content.startswith(b'h2'):
            file_content = file_content.replace(b'h2', b'HTTP/1.1', 1)
        self.file_content = file_content
        return self.file_content

    def get_json(self):
        try:
            return json.loads(self.data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def get_urlparse_query(self):
        try:
            query = parse_qs(urlparse(self.data.decode('utf-8')).query)
            query = {k: v[0] for k, v in query.items()}
            return query
        except UnicodeDecodeError:
            return None

    def print_json(self):
        print(json.dumps(self.json, indent=2, ensure_ascii=False))

    def print_urlparse(self):
        print(json.dumps(self.urlparse_query, indent=2, ensure_ascii=False))

    def print_readable(self):
        try:
            self.print_json()
        except json.JSONDecodeError:
            self.print_urlparse()


class ParseRequest:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_content = self.get_file_content()
        self.request = self._request_from_bytes(self.file_content)

        self.data = self.request.rfile.read()
        self.content_type = self.get_content_type()
        self.json = self.get_json()
        self.data_urlparse_query = self.get_data_urlparse_query()
        self.path = self.get_path()
        self.path_urlparse_query = self.get_path_urlparse_query()
        self.host = self.get_host()

    @staticmethod
    def _request_from_bytes(data):
        return HTTPRequest(data)

    def check_err(self):
        if self.request.error_code is not None:
            return False
        return True

    def get_content_type(self):
        content_type = self.request.headers.get('Content-Type')
        return content_type

    def get_file_content(self):
        with open(self.file_path, 'rb') as f:
            file_content = f.read()
        _f = file_content.split(b'\r\n', 1)[0]
        if _f.endswith(b'h2'):
            _f = _f[:-2] + b'HTTP/1.1'
            file_content = _f + b'\r\n' + file_content[len(_f):]
        self.file_content = file_content
        return self.file_content

    def get_host(self):
        host = self.request.headers['host']
        host = '' if host is None else host
        return host

    def get_path(self):
        path = self.request.path
        path = '' if path is None else path
        return path

    def get_no_query_path(self):
        return urlparse(self.path).path

    def get_path_urlparse_query(self):
        try:
            query = parse_qs(urlparse(self.path).query)
            query = {k: v[0] for k, v in query.items()}
            return query
        except AttributeError:
            logging.debug('[-] No query in request path')
            return None

    def get_data_urlparse_query(self):
        try:
            query = parse_qs(self.data.decode('utf-8'))
            query = {k: v[0] for k, v in query.items()}
            return query
        except (AttributeError, UnicodeDecodeError):
            logging.debug('[-] No query in request data')
            return None

    def get_json(self):
        try:
            return json.loads(self.data.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError):
            logging.debug('[-] Error: {} {}'.format(self.file_path, 'json decode error'))
            return None

    def print_path_urlparse(self):
        print(json.dumps(self.path_urlparse_query, indent=2, ensure_ascii=False))

    def print_json(self):
        print(json.dumps(self.json, indent=2, ensure_ascii=False))

    def print_readable(self):
        self.print_path_urlparse()
        self.print_json()

    def check_host(self, regx):
        if re.search(regx, self.host):
            return True
        return False

    def check_path(self, regx):
        if re.search(regx, self.path):
            return True
        return False


class ParseFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.req_path = None
        self.res_path = None
        self.type = self.check_type()
        self._parse_file_path()
        self.request = ParseRequest(self.req_path)
        self.response = ParseResponse(self.res_path)

    def check_type(self):
        if self.file_path.find('http_req') != -1:
            return 'req'
        elif self.file_path.find('http_res') != -1:
            return 'res'
        else:
            return 'unknown'

    def _parse_file_path(self):
        if self.type == 'req':
            self.req_path = self.file_path
            self.res_path = self.file_path.replace('http_req', 'http_res')
        elif self.type == 'res':
            self.req_path = self.file_path.replace('http_res', 'http_req')
            self.res_path = self.file_path
        else:
            logging.debug('[-] Error: {}'.format(self.file_path))
        return


class ParseDir:
    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.files = self.get_files()
        self.parse_files = self.get_parse_files()

    def get_files(self):
        files = []
        for file in os.listdir(self.dir_path):
            if file.startswith('http_res'):
                files.append(file)
        return files

    def get_parse_files(self):
        parse_files = []
        for file in self.files:
            file_path = os.path.join(self.dir_path, file)
            # try:
            parse_file = ParseFile(file_path)
            parse_files.append(parse_file)
            # except Exception as e:
            #     logging.debug('[-] Error: {} - {}'.format(file_path, e))
            #     continue
        return parse_files

    def filter_req_host(self, regx):
        parse_files = []
        for parse_file in self.parse_files:
            if parse_file.request.check_host(regx):
                parse_files.append(parse_file)
        return parse_files

    def filter_req_path(self, regx):
        parse_files = []
        for parse_file in self.parse_files:
            if parse_file.request.check_path(regx):
                parse_files.append(parse_file)
        return parse_files
