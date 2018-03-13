from flask import Blueprint, request, Response
import os
import requests
from ruamel.yaml import YAML

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "filemaker.yml")) as config_file:
    yaml = YAML()
    filemaker_config = yaml.load(config_file)

filemaker = Blueprint("filemaker", __name__)


@filemaker.route("/", methods=["GET", "POST", "PUT", "DELETE"])
@filemaker.route("/<path:filemaker_request>", methods=["GET", "POST", "PUT", "DELETE"])
def proxy_to_filemaker(filemaker_request=None):
    if filemaker_request is None:
        request_url = filemaker_config["base_url"]
    else:
        request_url = "{}{}".format(filemaker_config["base_url"], filemaker_request)
    resp = requests.request(
        method=request.method,
        url=request_url,
        headers={key: value for (key, value) in request.headers if key != "Host"},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        verify=False
    )

    excluded_headers = ["content-encoding", "content-length", "transfer-encoding", "connection"]
    headers = [(name, value) for (name, value) in resp.raw.headers.items() if name.lower() not in excluded_headers]

    return Response(resp.content, resp.status_code, headers)
