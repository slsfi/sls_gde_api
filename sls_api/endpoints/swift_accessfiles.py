from __future__ import unicode_literals
from flask import abort, Blueprint, Response
import mimetypes
import os
from io import BytesIO
import requests
import traceback
import yaml
import urllib3
urllib3.disable_warnings()  # TODO signed cert for Isilon Swift

swift = Blueprint("swift", __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
derivate_objects_list_file = os.path.join(config_dir, "derivate_objects_list.txt")
with open(os.path.join(config_dir, "swift_auth.yml")) as swift_auth_file:
    swift_auth = yaml.load(swift_auth_file)

with open(derivate_objects_list_file) as list_file:
    valid_files = set()
    for line in list_file:
        valid_files.add(line.strip())


@swift.route('/<path:file_path>', methods=["GET"])
def get_file_or_404(file_path):
    """
    Gets a file from ISILON Swift, if the file is on the configured list of allowed files, otherwise returns 404
    """
    file_obj, mime_type = get_file_if_on_list(file_path)
    if file_obj is None:
        abort(404)
    else:
        return Response(file_obj, status=200, mimetype=mime_type, content_type=mime_type)


def get_file_if_on_list(filename):
    mime_type = mimetypes.guess_type(filename)[0]
    if mime_type is None:
        mime_type = "application/octet-stream"  # Unknown file type, arbitrary binary data

    try:
        if filename in valid_files:
            return _actually_get_file_from_swift(filename), mime_type
        else:
            print("Filename {!r} not in list of approved files!".format(filename))
            return None, None
    except Exception:
        print("Exception when getting file from Swift!")
        print(traceback.format_exc())
        return None, None


def _actually_get_file_from_swift(path):
    print("All checks OK, getting file {!r} from Swift".format(path))
    with requests.Session() as s:
        auth_url = swift_auth["auth_url"]
        username = swift_auth["username"]
        password = swift_auth["password"]

        s, storage_url = _authenticate_to_swift(auth_url, s, username, password)

        ret = s.get("{}/{}".format(storage_url, path), verify=False, stream=True)

        if ret.status_code == 200:
            output = BytesIO()
            output.write(ret.content)
            content = output.getvalue()
            output.close()
            return content


def _authenticate_to_swift(url, session, user, key, debug_prints=False):
    auth = {
        "X-Auth-User": user,
        "X-Auth-Key": key
    }
    session.headers.update(auth)

    r = session.get(url, verify=False)
    if debug_prints:
        print("--- Getting AUTH token --- GET {}".format(url))
        print(r.status_code)
        print(r.headers)
        print(r.content)

    token = r.headers["X-Auth-Token"]
    storage_url = r.headers["X-Storage-Url"]
    del session.headers["X-Auth-User"]
    del session.headers["X-Auth-Key"]
    session.headers.update({
        "X-Auth-Token": token
    })

    return session, storage_url
