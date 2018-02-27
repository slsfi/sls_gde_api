from flask import Blueprint, request, Response
import os
import requests
from ruamel.yaml import YAML

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "filemaker.yml")) as config_file:
    yaml = YAML()
    filemaker_config = yaml.load(config_file)

filemaker = Blueprint("filemaker", __name__)


@filemaker.route("/<path:filemaker_request>", methods=["GET", "POST", "PUT", "DELETE"])
def send_request(filemaker_request):
    # TODO certificate verification
    print("{}{}".format(filemaker_config["base_url"], filemaker_request))
    if request.method == "GET":
        fm_return = requests.get("{base_url}{path}".format(base_url=filemaker_config["base_url"], path=filemaker_request),
                                 headers=request.headers, verify=False)
        return Response(fm_return.content, status=fm_return.status_code, content_type="application/json")
    elif request.method == "POST":
        fm_return = requests.post("{base_url}{path}".format(base_url=filemaker_config["base_url"], path=filemaker_request),
                                  headers=request.headers, json=request.json, verify=False)
        return Response(fm_return.content, status=fm_return.status_code, content_type="application/json")
    elif request.method == "PUT":
        fm_return = requests.put("{base_url}{path}".format(base_url=filemaker_config["base_url"], path=filemaker_request),
                                 headers=request.headers, data=request.json, verify=False)
        return Response(fm_return.content, status=fm_return.status_code, content_type="application/json")
    elif request.method == "DELETE":
        fm_return = requests.delete("{base_url}{path}".format(base_url=filemaker_config["base_url"], path=filemaker_request),
                                    headers=request.headers, verify=False)
        return Response(fm_return.content, status=fm_return.status_code, content_type="application/json")
