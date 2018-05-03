from collections import OrderedDict
from dateutil.parser import parse
import datetime
from flask import Blueprint, Response, request
from lxml.etree import Element, SubElement, tostring
import os
from sqlalchemy import create_engine
import sqlalchemy.sql
from ruamel.yaml import YAML
import traceback

from sls_api.endpoints.oai import valid_OAI_verbs, create_error_xml, create_element, create_split_element

oai = Blueprint("oai_library", __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "oai_library.yml")) as config_file:
    yaml = YAML()
    oai_config = yaml.load(config_file)


@oai.route('/', methods=["GET"])
def process_oai_request():
    base_url = request.base_url

    valid_params, error = validate_request(request)

    if error is None:
        # Create root OAI-PMH element
        root_element = create_root_element(valid_params["verb"])

        # Add responseDate
        create_element(root_element, "responseDate", datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

        # Add request element with request metadata
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
        create_element(root_element, "request", base_url, attributes=attrs)

        try:
            engine = create_engine(oai_config["engine"])

            # Add result container element and fill with metadata response
            result_tag = valid_params["verb"]

            container_root = SubElement(root_element, result_tag)
            if result_tag == "Identify":
                # Get earliest datestamp from database and populate Identify element
                connection = engine.connect()
                statement = sqlalchemy.sql.text("SELECT MIN({date}) AS date FROM {table}".format(date=oai_config["date_column"], table=oai_config["table"]))
                result = parse(connection.execute(statement).fetchone()["date"])
                populate_identify_element(base_url, container_root, result)
                connection.close()
            elif result_tag == "ListMetadataFormats":
                populate_listmetadataformats_element(container_root)
            elif result_tag == "ListSets":
                populate_listsets_element(container_root)
            else:
                if result_tag == "ListIdentifiers":
                    connection = engine.connect()
                    statement = build_sql_statement("SELECT {ident}, {date} FROM {table}", valid_params)
                    result = connection.execute(statement).fetchall()
                    if len(result) == 0:
                        error = ("noRecordsMatch", "No records match the given query.")
                    else:
                        for row in result:
                            header_elem = SubElement(container_root, "header")
                            create_element(header_elem, "identifier", row[oai_config["id_column"]])
                            create_element(header_elem, "datestamp", parse(row[oai_config["date_column"]]))
                            for setSpec, _ in oai_config["sets"].items():
                                create_element(header_elem, "setSpec", setSpec)
                    connection.close()
                else:
                    # ListRecords & GetRecord
                    connection = engine.connect()
                    if result_tag == "ListRecords":
                        statement = build_sql_statement("SELECT * FROM {table}", valid_params)
                    else:
                        statement = build_sql_statement("SELECT * FROM {table} WHERE {ident} = :ident", valid_params)
                    result = connection.execute(statement).fetchall()
                    if len(result) == 0 and result_tag == "ListRecords":
                        error = ("noRecordsMatch", "No records match the given query.")
                    elif len(result) == 0:
                        error = ("idDoesNotExist", "The given ID could not be found in the database.")
                    else:
                        namespace_map = root_element.nsmap.copy()
                        for ns, url in oai_config["namespace_map"].items():
                            namespace_map[ns] = url

                        for row in result:
                            row = dict(row)
                            record_root = SubElement(container_root, "record")
                            header_elem = SubElement(record_root, "header")
                            create_element(header_elem, "identifier", row[oai_config["id_column"]])
                            create_element(header_elem, "datestamp", parse(row[oai_config["date_column"]]))
                            for setSpec, _ in oai_config["sets"].items():
                                create_element(header_elem, "setSpec", setSpec)

                            metadata_root = SubElement(record_root, "metadata")
                            oai_dc_root = SubElement(metadata_root, "{%s}dc" % namespace_map["oai_dc"])

                            for column, element_tag in oai_config["record_map"].items():
                                ns, element_tag = element_tag.split(":")
                                if element_tag == "subject":
                                    create_split_element(oai_dc_root, "{%s}%s" % (namespace_map[ns], element_tag), row[column])
                                else:
                                    create_element(oai_dc_root, "{%s}%s" % (namespace_map[ns], element_tag), row[column])
                    connection.close()
        except Exception:
            print(traceback.format_exc())
            error = ("InternalError", "Could not retrieve metadata from database.")

        if error:
            return_content = create_error_xml(base_url, valid_params["verb"] if "verb" in valid_params else None, error[0], error[1])
        else:
            return_content = tostring(root_element, encoding="UTF-8", pretty_print=True, xml_declaration=True)

        if not error:
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

        return Response(response=return_content, status=return_status, content_type="text/xml; charset=utf-8")


def build_sql_statement(base_statement, valid_params):
    from_sql = ""
    if "from" in valid_params:
        from_sql = "{date} >= :from_date".format(date=oai_config["date_column"])
    until_sql = ""
    if "until" in valid_params:
        until_sql = "{date} <= :until_date".format(date=oai_config["date_column"])
    if from_sql or until_sql:
        base_statement += " WHERE"
        for sub_statement in (from_sql, until_sql):
            if sub_statement:
                if base_statement.split(" ")[-1] == "WHERE":
                    base_statement += " {}".format(sub_statement)
                else:
                    base_statement += " AND {}".format(sub_statement)

    statement = base_statement.format(ident=oai_config["id_column"], date=oai_config["date_column"], table=oai_config["table"])
    from_date = valid_params.get("from", None)
    until_date = valid_params.get("until", None)
    ident = valid_params.get("identifier", None)
    sql_statment = sqlalchemy.sql.text(statement)
    if ":from_date" in statement:
        sql_statment = sql_statment.bindparams(from_date=from_date)
    if ":until_date" in statement:
        sql_statment = sql_statment.bindparams(until_date=until_date)
    if ":ident" in statement:
        sql_statment = sql_statment.bindparams(ident=ident)
    return sql_statment


def create_root_element(verb):
    root_tag = "OAI-PMH"

    xmlns = "http://www.openarchives.org/OAI/2.0/"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    oai_dc = "http://www.openarchives.org/OAI/2.0/oai_dc/"
    dc = "http://purl.org/dc/elements/1.1/"
    dcterms = "http://purl.org/dc/terms/"

    namespace_map = OrderedDict()
    if verb in ["ListRecords", "ListIdentifiers", "GetRecord"]:
        # Record elements will use most of the namespaces
        namespace_map[None] = xmlns
        namespace_map["oai_dc"] = oai_dc
        namespace_map["dc"] = dc
        namespace_map["xsi"] = xsi
        namespace_map["dcterms"] = dcterms
    else:
        # Identify only needs the None and xsi namespaces
        namespace_map[None] = xmlns
        namespace_map["xsi"] = xsi

    root_attrs = OrderedDict()
    root_attrs["{%s}schemaLocation" % namespace_map["xsi"]] = "http://dublincore.org/schemas/xmls/qdc/dcterms.xsd"

    return Element("{%s}%s" % (namespace_map[None], root_tag), root_attrs, nsmap=namespace_map)


def populate_identify_element(base_url, root_xml, earliest_date_stamp):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'Identify' verb spec
    """
    identify_content = OrderedDict()
    identify_content["repositoryName"] = oai_config["repo_name"]
    identify_content["baseURL"] = base_url
    identify_content["protocolVersion"] = oai_config["repo_protocol_ver"]
    identify_content["adminEmail"] = oai_config["repo_admin"]
    identify_content["earliestDatestamp"] = earliest_date_stamp
    identify_content["deletedRecord"] = oai_config["repo_deletion"]
    identify_content["granularity"] = oai_config["repo_granularity"]

    for key, value in identify_content.items():
        create_element(root_xml, key, value)


def populate_listsets_element(root_xml):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'ListSets' spec
    """
    for setSpec, setName in oai_config["sets"].items():
        set_element = SubElement(root_xml, "set")
        create_element(set_element, "setSpec", setSpec)
        create_element(set_element, "setName", setName)


def populate_listmetadataformats_element(root_xml):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'ListMetadataFormats' spec
    """
    oai_dc_root = SubElement(root_xml, "metadataFormat")
    create_element(oai_dc_root, "metadataPrefix", "dcterms")
    create_element(oai_dc_root, "schema", "http://dublincore.org/schemas/xmls/qdc/dcterms.xsd")
    create_element(oai_dc_root, "metadataNamespace", "http://purl.org/dc/terms/")


def validate_request(req):
    """
    Validates a Flask request to ensure it conforms to the OAI-PMH standard
    """
    valid_params = {}
    error = None

    for key, value in req.args.items():
        if key not in ["verb", "from", "until", "identifier", "set", "metadataPrefix"]:
            error = ("badArgument", "Unknown argument")
    if error is not None:
        return valid_params, error
    if "verb" in req.args and req.args["verb"] in valid_OAI_verbs:
        valid_params["verb"] = req.args["verb"]
    else:
        error = ("badVerb", "Bad OAI verb")

    if "from" in req.args and error is None:
        try:
            from_date = parse(req.args["from"])
        except Exception:
            error = ("badArgument", "From-date malformed")
        else:
            valid_params["from"] = from_date.strftime("%Y-%m-%d")

    if "until" in req.args and error is None:
        try:
            until_date = parse(req.args["until"])
        except Exception:
            error = ("badArgument", "Until-date malformed")
        else:
            valid_params["until"] = until_date.strftime("%Y-%m-%d")

    if "identifier" in req.args and error is None:
        valid_params["identifier"] = req.args["identifier"]

    if "set" in req.args and error is None:
        valid = False
        for setSpec, setName in oai_config["sets"].items():
            if req.args["set"] == setSpec:
                valid_params["set"] = setSpec
                valid_params["setName"] = setName
                valid = True
        if not valid:
            error = ("badArgument", "Unknown set")

    if "metadataPrefix" in req.args and error is None:
        prefix = req.args["metadataPrefix"]
        if prefix in ["oai_dc"]:
            valid_params["metadataPrefix"] = prefix
        else:
            error = ("cannotDisseminateFormat", "Unknown metadata prefix")

    if error is None:
        # Sanity checks
        if "verb" in valid_params:
            # Identify and ListSets - No other parameters with verb
            if valid_params["verb"] == "Identify" or valid_params["verb"] == "ListSets":
                if len(valid_params) > 1:
                    error = ("badArgument", "No other parameters with {}".format(valid_params["verb"]))
            # ListMetadataFormats - only identifier parameter is valid, and it is optional
            elif valid_params["verb"] == "ListMetadataFormats":
                if ("identifier" not in valid_params and len(valid_params) > 1) or ("identifier" in valid_params and len(valid_params) > 2):
                    error = ("badArgument", "Only identifier is a valid parameter for {}".format(valid_params["verb"]))
            # ListIdentifiers and ListRecords - metadataPrefix param is required, identifier param is invalid, all others valid but optional
            elif valid_params["verb"] == "ListIdentifiers" or valid_params["verb"] == "ListRecords":
                if "metadataPrefix" not in valid_params:
                    error = ("badArgument", "metadataPrefix is missing")
                elif "identifier" in valid_params:
                    error = ("badArgument", "The identifier parameter is not allowed for {}".format(valid_params["verb"]))
            # GetRecord -  requires metadataprefix and identifier, no other parameters are valid
            elif valid_params["verb"] == "GetRecord":
                if "metadataPrefix" not in valid_params or "identifier" not in valid_params or len(valid_params) != 3:
                    error = ("badArgument", "metadataPrefix and identifier parameters are required, no other parameters are valid")

    return valid_params, error
