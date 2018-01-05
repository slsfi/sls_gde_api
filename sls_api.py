from __future__ import unicode_literals
from flask import Flask, abort, Response, request

from endpoints.oai import process_oai_request
from endpoints.swift_accessfiles import get_file_if_on_list

app = Flask(__name__)


@app.route('/accessfiles/<path:file_path>', methods=["GET"])
def get_file_or_404(file_path):
    """
    Gets a file from ISILON Swift, if the file is on the configured list of allowed files
    """
    file_obj, mime_type = get_file_if_on_list(file_path)
    if file_obj is None:
        abort(404)
    else:
        return Response(file_obj, status=200, mimetype=mime_type, content_type=mime_type)


@app.route('/oai/', methods=["GET"])
def get_oai_metadata():
    """
    Provides OAI-PMH metadata for Finna/Europeana Arkiva data
    """
    response = process_oai_request(request)
    return response


if __name__ == '__main__':
    app.run()
