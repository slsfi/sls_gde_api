from __future__ import unicode_literals
from collections import OrderedDict
import datetime
from flask import Flask, abort, Response, request
from lxml.etree import SubElement, tostring


from endpoints.oai import validate_request, get_metadata_from_mysql, create_root_element, \
    populate_identify_element, populate_listmetadataformats_element, populate_listsets_element, \
    populate_ead_records_element, populate_records_element, create_error_xml
from endpoints.swift_interface import get_file_or_none

app = Flask(__name__)


@app.route('/accessfiles/<path:file_path>', methods=["GET"])
def get_file_or_404(file_path):
    file_obj, mime_type = get_file_or_none(file_path)
    if file_obj is None:
        abort(404)
    else:
        return Response(file_obj, status=200, mimetype=mime_type, content_type=mime_type)


@app.route('/oai/', methods=["GET"])
def get_oai_metadata():
    base_url = request.base_url

    valid_params, error = validate_request(request)

    result, error = get_metadata_from_mysql(valid_params, error)

    if error is not None:
        if "verb" in valid_params:
            return_content = create_error_xml(base_url, valid_params["verb"], *error)
        else:
            return_content = create_error_xml(base_url)
    else:
        # First, create the root OAI-PMH element
        root_element = create_root_element(valid_params["verb"])

        # Then, add the responseDate element
        response_date_element = SubElement(root_element, "responseDate")
        response_date_element.text = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Next, add the request element
        attrs = OrderedDict()
        attrs["verb"] = valid_params["verb"]
        if valid_params["verb"] in ["ListRecords", "ListIdentifiers", "GetRecord"]:
            attrs["metadataPrefix"] = valid_params["metadataPrefix"]
            if "from" in valid_params:
                attrs["from"] = valid_params["from"]
            if "until" in valid_params:
                attrs["until"] = valid_params["until"]
            if "set" in valid_params:
                attrs["set"] = valid_params["set"]
        if "identifier" in valid_params:
            attrs["identifier"] = valid_params["identifier"]
        request_element = SubElement(root_element, "request", attrs)
        request_element.text = base_url

        # Then add the result container element and fill it with the result dict data
        result_tag = valid_params["verb"]

        if result_tag == "Identify":
            # Create and populate Identify element
            identify_root = SubElement(root_element, result_tag)
            date_byte_string = result[0]["date"]
            populate_identify_element(base_url, identify_root, date_byte_string.decode("utf-8", errors="ignore"))
        elif result_tag == "ListMetadataFormats":
            # Create and populate ListMetadataFormats element
            listmetadataformats_root = SubElement(root_element, result_tag)
            populate_listmetadataformats_element(listmetadataformats_root)
        elif result_tag == "ListSets":
            # Create and populate ListSets element
            listsets_root = SubElement(root_element, result_tag)
            populate_listsets_element(listsets_root)
        else:
            # ListIdentifiers, ListRecords, or GetRecord
            # Create container element for records and fill it with record elements according to metadataPrefix
            records_root = SubElement(root_element, result_tag)
            if valid_params["metadataPrefix"] == "ead":
                for row_dict in result:
                    populate_ead_records_element(records_root, row_dict, valid_params["metadataPrefix"], valid_params["verb"])
            else:
                for row_dict in result:
                    populate_records_element(records_root, row_dict, valid_params["metadataPrefix"], valid_params["verb"])

        # Turn the generated XML tree into a utf8-encoded indented string
        return_content = tostring(root_element, encoding="utf-8", pretty_print=True)

    # TODO check if non-200 status breaks XML rendering on common browsers, if so just return 200 always
    if error is None:
        return_status = 200
    else:
        if error[0] in ["internalError", "databaseError"]:
            # Return 500 - internal error. Browser should still render XML.
            return_status = 500
        elif error[0] in ["idDoesNotExist", "noRecordsMatch"]:
            # Return 404 - Not Found. Browser should still render XML.
            return_status = 404
        else:
            # Return 400 - Bad Request. Browser... should still render XML?
            return_status = 400

    return return_content, return_status, {"Content-Type": "application/xml; charset=utf-8", "Content-Length": 0 if return_content is None else len(return_content)}


if __name__ == '__main__':
    app.run()
