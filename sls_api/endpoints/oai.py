# coding=utf-8
from __future__ import unicode_literals
from collections import OrderedDict
import datetime
from dateutil.parser import parse
from flask import Blueprint, Response, request
from lxml.etree import Element, SubElement, tostring
import os
import pymysql
import traceback
import yaml

oai = Blueprint("oai", __name__)

valid_OAI_verbs = ["Identify", "ListSets", "ListMetadataFormats", "ListIdentifiers", "ListRecords", "GetRecord"]
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "oai.yml")) as mysql_config_file:
    mysql_config = yaml.load(mysql_config_file)
accessfile_API_endpoint = "http://api.sls.fi/accessfiles/"  # TODO get endpoint programmatically
# Define MySQL connection variable as module-level global, so we can use it in multiple places once opened
connection = None


@oai.route('/', methods=["GET"])
def process_oai_request():
    """
    Fully processes an incoming Flask request
    Returns a Flask Response containing a string of XML containing metadata or an error message
    """
    base_url = request.base_url

    # First, ensure request is valid
    valid_params, error = validate_request(request)

    result = []
    if error is None:
        try:
            open_mysql_connection()
        except Exception:
            print(traceback.format_exc())
            error = ("internalError", "Could not connect to metadata database")
        else:
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
        create_element(root_element, "responseDate", datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))

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
        create_element(root_element, "request", base_url, attributes=attrs)

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
                    populate_ead_records_element(records_root, row_dict, valid_params["verb"])
            else:
                for row_dict in result:
                    populate_records_element(records_root, row_dict, valid_params["metadataPrefix"],
                                             valid_params["verb"])

        # Turn the generated XML tree into a utf8-encoded indented string
        return_content = tostring(root_element, encoding="UTF-8", pretty_print=True, xml_declaration=True)

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
    # Ensure the MySQL connection is closed before returning, so that the connection won't be kept alive until timeout
    if connection is not None and connection.open:
        connection.close()
    return Response(response=return_content, status=return_status, content_type="text/xml; charset=utf-8")


def validate_request(req):
    """
    Validates a Flask request to ensure it conforms to the OAI-PMH standard
    """
    valid_params = {}
    error = None

    for key, value in req.args.iteritems():
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
        if req.args["set"] == "SLSeuropeana":
            valid_params["set"] = req.args["set"]
            valid_params["setName"] = "SLS material till Europeana"
        elif req.args["set"] == "SLSfinna":
            valid_params["set"] = req.args["set"]
            valid_params["setName"] = "SLS material till Finna/NDB"
        else:
            error = ("badArgument", "Unknown set")

    if "metadataPrefix" in req.args and error is None:
        prefix = req.args["metadataPrefix"]
        if prefix in ["oai_dc", "europeana", "ead"]:
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


def create_element(parent, tag_name, contents, attributes=None, namespace_map=None):
    """
    Creates an XML SubElement of the 'parent' XML element
    """
    if contents:
        new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
        new_elem.text = contents.strftime("%Y-%m-%d") if isinstance(contents, (datetime.date, datetime.datetime)) else contents
        return new_elem


def create_empty_element(parent, tag_name, attributes=None, namespace_map=None):
    """
    Create an XML SubElement with an empty string for a body
    Should create a full-width element rather than a compact one
    This is usefule to follow specs
    """
    new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
    new_elem.text = ""
    return new_elem


def create_split_element(parent, tag_name, contents, attributes=None, namespace_map=None, delimiter=", "):
    """
    Create one or more XML SubElements of the 'parent' XML element
    Splits the 'contents' parameter using the 'delimiter' parameter and creates one SubElement for each part
    If the 'contents' parameter doesn't contain the 'delimiter' parameter, only one SubElement is created
    """
    if contents:
        if delimiter in contents:
            split_contents = contents.split(delimiter)
            for content in split_contents:
                if content:
                    new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
                    new_elem.text = content
        else:
            new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
            new_elem.text = contents


def open_mysql_connection():
    """
    Open a MySQL connection, storing it in a module-level variable 'connection'
    """
    global connection
    connection = pymysql.connect(host=mysql_config["address"],
                                 user=mysql_config["username"],
                                 password=mysql_config["password"],
                                 db=mysql_config["database"],
                                 charset="utf8",
                                 cursorclass=pymysql.cursors.DictCursor)


def get_metadata_from_mysql(valid_params, error):
    """
    Get needed metadata from MySQL using a pymysql connection
    """
    result = []
    if error is None:
        sql_query = generate_sql_query(valid_params)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchall()

            if (valid_params["verb"] == "GetRecord" or (valid_params["verb"] == "ListMetadataFormats" and "identifier" in valid_params)) and len(result) == 0:
                error = ("idDoesNotExist", "No record with that id could be found")
            elif (valid_params["verb"] == "ListRecords" or valid_params["verb"] == "ListIdentifiers") and len(result) == 0:
                error = ("noRecordsMatch", "No records match the criteria given")
        except Exception:
            error = ("databaseError", "An error occurred when querying the database")
            print(traceback.format_exc())
    return result, error


def create_root_element(verb):
    """
    Create the root element for the OAI-PMH XML return
    """
    root_tag = "OAI-PMH"

    # List of all the namespaces
    xmlns = "http://www.openarchives.org/OAI/2.0/"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    oai_dc = "http://www.openarchives.org/OAI/2.0/oai_dc/"
    ead = "http://www.loc.gov/ead"
    dc = "http://purl.org/dc/elements/1.1/"
    dcterms = "http://purl.org/dc/terms/"
    europeana = "http://www.europeana.eu/schemas/ese/"

    namespace_map = OrderedDict()
    if verb in ["ListRecords", "ListIdentifiers", "GetRecord"]:
        # Record elements will use most of the namespaces
        namespace_map[None] = xmlns
        namespace_map["oai_dc"] = oai_dc
        namespace_map["ead"] = ead
        namespace_map["dc"] = dc
        namespace_map["xsi"] = xsi
        namespace_map["dcterms"] = dcterms
        namespace_map["europeana"] = europeana
    else:
        # Identify only needs the None and xsi namespaces
        namespace_map[None] = xmlns
        namespace_map["xsi"] = xsi

    root_attrs = OrderedDict()
    root_attrs["{%s}schemaLocation" % namespace_map["xsi"]] = "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"

    return Element("{%s}%s" % (namespace_map[None], root_tag), root_attrs, nsmap=namespace_map)


def populate_identify_element(base_url, root_xml, earliest_date_stamp):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'Identify' verb spec
    """
    # TODO move identify elements to a settings file
    identify_content = OrderedDict()
    identify_content["repositoryName"] = "SLS/Arkiva"
    identify_content["baseURL"] = base_url
    identify_content["protocolVersion"] = "2.0"
    identify_content["adminEmail"] = "is@sls.fi"
    identify_content["earliestDatestamp"] = earliest_date_stamp
    identify_content["deletedRecord"] = "persistent"
    identify_content["granularity"] = "YYYY-MM-DD"

    for key, value in identify_content.items():
        create_element(root_xml, key, value)


def populate_listsets_element(root_xml):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'ListSets' spec
    """
    europeana_set_element = SubElement(root_xml, "set")
    create_element(europeana_set_element, "setSpec", "SLSeuropeana")
    create_element(europeana_set_element, "setName", "SLS material till Europeana")

    finna_set_element = SubElement(root_xml, "set")
    create_element(finna_set_element, "setSpec", "SLSfinna")
    create_element(finna_set_element, "setName", "SLS material till Finna/NDB")


def populate_listmetadataformats_element(root_xml):
    """
    Populate the 'root_xml' XML Element with SubElements according to the OAI 'ListMetadataFormats' spec
    """
    oai_dc_root = SubElement(root_xml, "metadataFormat")
    create_element(oai_dc_root, "metadataPrefix", "oai_dc")
    create_element(oai_dc_root, "schema", "http://www.openarchives.org/OAI/2.0/oai_dc.xsd")
    create_element(oai_dc_root, "metadataNamespace", "http://www.openarchives.org/OAI/2.0/oai_dc/")

    europeana_root = SubElement(root_xml, "metadataFormat")
    create_element(europeana_root, "metadataPrefix", "europeana")
    create_element(europeana_root, "schema", "http://www.europeana.eu/schemas/ese/ESE-V3.4.xsd")
    create_element(europeana_root, "metadataNamespace", "http://www.europeana.eu/schemas/ese/")

    ead_root = SubElement(root_xml, "metadataFormat")
    create_element(ead_root, "metadataPrefix", "ead")
    create_element(ead_root, "schema", "http://www.loc.gov/ead/ead.xsd")
    create_element(ead_root, "metadataNamespace", "http://www.loc.gov/ead")


def populate_records_element(root_xml, record_dict, metadata_prefix, verb):
    """
    Depending on verb (ListIdentifiers, ListRecords, GetRecord) and metadata_prefix (europeana, oai_dc)
    Populate the 'root_xml' XML Element with SubElements according to the OAI spec for metadata Identifiers/Records
    """
    if verb != "ListIdentifiers":
        record = SubElement(root_xml, "record")
        element = SubElement(record, "header")
    else:
        record = SubElement(root_xml, "header")
        element = record
    create_element(element, "identifier", record_dict["identifier"])
    create_element(element, "datestamp", record_dict["date_modified"])

    if record_dict["to_europeana"]:
        create_element(element, "setSpec", "SLSeuropeana")
    if record_dict["to_ndb"]:
        create_element(element, "setSpec", "SLSfinna")
    if record_dict["status"] == "deleted":
        element.attrib["status"] = "deleted"
    elif verb == "ListRecords" or verb == "GetRecord":
        # Add record metadata, lots of it.
        namespace_map = root_xml.nsmap.copy()
        xml = "http://www.w3.org/XML/1998/namespace"
        namespace_map["xml"] = xml
        dc = namespace_map["dc"]
        dcterms = namespace_map["dcterms"]
        element = SubElement(record, "metadata")

        if metadata_prefix == "europeana":
            container = SubElement(element, "{%s}record" % namespace_map["europeana"])
        else:
            container = SubElement(element, "{%s}dc" % namespace_map["oai_dc"])

        # dc:title
        create_element(container, "{%s}title" % dc, record_dict["dc_title"])

        # dc:type
        create_element(container, "{%s}type" % dc, record_dict["dc_type2"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:type
        if record_dict["DC2_type"] and record_dict["DC2_type"].lower() == "sound":
            create_element(container, "{%s}type" % dc, record_dict["entity_label"],
                           attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:type
        create_element(container, "{%s}type" % dc, record_dict["dc_type2_eng"],
                       attributes={"{%s}lang" % xml: "en"}, namespace_map=namespace_map)

        # dc:subject
        create_split_element(container, "{%s}subject" % dc, record_dict["dc_subject"],
                             attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:description
        create_element(container, "{%s}description" % dc, record_dict["dc_description"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:description
        if record_dict["DC2_type"] and record_dict["DC2_type"].lower() == "text":
            create_element(container, "{%s}description" % dc, record_dict["entity_label"],
                           attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:source
        create_element(container, "{%s}source" % dc, record_dict["dc_source"])

        if metadata_prefix == "europeana":
            # dcterms:isPartOf
            create_element(container, "{%s}isPartOf" % dcterms, record_dict["arkivetsNamn"])
            create_element(container, "{%s}isPartOf" % dcterms, record_dict["c_signum"])

            # dcterms:spatial
            for i in range(1, 5):
                create_split_element(container, "{%s}spatial" % dcterms, record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"],
                                     attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

            # dctrems:created
            create_element(container, "{%s}created" % dcterms, record_dict["dcterms_created_maskinlasbart"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

            # dcterms:isReferencedBy
            create_element(container, "{%s}isReferencedBy" % dcterms, record_dict["dcterms_isReferencedBy"])

            # dcterms:isFormatOf
            create_element(container, "{%s}isFormatOf" % dcterms, record_dict["dc_identifier"])

            # dcterms:extent
            create_element(container, "{%s}extent" % dcterms,
                           record_dict["dc_source_dimensions"] if record_dict["dc_source_dimensions"] else record_dict["duration"])

            # dcterms:medium
            create_element(container, "{%s}medium" % dcterms, record_dict["dc_source2"])

        else:
            # dc:relation
            create_element(container, "{%s}relation" % dc, record_dict["arkivetsNamn"])
            create_element(container, "{%s}relation" % dc, record_dict["c_signum"])

            # dc:coverage
            for i in range(1, 5):
                create_split_element(container, "{%s}coverage" % dc,
                                     record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"],
                                     attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

            # dc:date
            create_element(container, "{%s}date" % dc, record_dict["dcterms_created_maskinlasbart"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

            # dc:relation
            create_element(container, "{%s}relation" % dc, record_dict["dcterms_isReferencedBy"])
            create_element(container, "{%s}relation" % dc, record_dict["dc_identifier"])

            # dc:format
            create_element(container, "{%s}format" % dc,
                           record_dict["dc_source_dimensions"] if record_dict["dc_source_dimensions"] else record_dict["duration"])

            create_element(container, "{%s}format" % dc, record_dict["dc_source2"])

        # dc:format
        create_element(container, "{%s}format" % dc, record_dict["filetype_MIME"],
                       attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:IMT"})

        # dc:creator
        create_element(container, "{%s}creator" % dc, record_dict["dc_creator"])

        # dc:publisher
        # join together the two publisher fields
        publisher = ""
        if record_dict["dc_publisher"]:
            publisher += record_dict["dc_publisher"]
        if record_dict["dc_publisher2"]:
            if len(publisher) > 0:
                publisher += ", "
            publisher += record_dict["dc_publisher2"]
        create_element(container, "{%s}publisher" % dc, publisher)

        # dc:rights
        create_element(container, "{%s}rights" % dc, record_dict["dc_rights"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dcterms:issued / dc:date
        if metadata_prefix == "europeana":
            create_element(container, "{%s}issued" % dcterms, record_dict["DCterms_issued"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
        else:
            create_element(container, "{%s}date" % dc, record_dict["DCterms_issued"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

        # dc:identifier
        create_element(container, "{%s}identifier" % dc, record_dict["identifier"])

        # dc:language
        create_element(container, "{%s}language" % dc, record_dict["dc_language"],
                       attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:ISO639-2"})

        if metadata_prefix == "europeana":
            # europeana namespace
            # object
            if record_dict["derivate_filepath"]:
                create_element(container, "{%s}object" % namespace_map["europeana"],
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]))

            # provider
            create_element(container, "{%s}provider" % namespace_map["europeana"], "National Formula agreement")

            # type
            create_element(container, "{%s}type" % namespace_map["europeana"], record_dict["ESE_type"])

            # rights
            create_element(container, "{%s}rights" % namespace_map["europeana"], record_dict["europeanaRights"])

            # dataProvider
            create_element(container, "{%s}dataProvider" % namespace_map["europeana"], "Svenska litteratursällskapet i Finland")

            # isShownBy
            if record_dict["derivate_filepath"]:
                create_element(container, "{%s}isShownBy" % namespace_map["europeana"],
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]))
            # isShownAt
            if record_dict["DC2_type"] and record_dict["DC2_type"].lower() == "sound":
                create_element(container, "{%s}isShownAt" % namespace_map["europeana"], record_dict["c_isReferencedBy_URL"])

        else:
            # dc:identifier
            if record_dict["derivate_filepath"]:
                create_element(container, "{%s}identifier" % dc,
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]),
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})

            # dc:publisher
            create_element(container, "{%s}publisher" % dc, "National Formula agreement")

            # dc:type
            create_element(container, "{%s}type" % dc, record_dict["ESE_type"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:DCMItype"})

            # dc:rights
            create_element(container, "{%s}rights" % dc, record_dict["europeanaRights"])

            # dc:publisher
            create_element(container, "{%s}publisher" % dc, "Svenska litteratursällskapet i Finland")

            # dc:identifier
            if record_dict["derivate_filepath"]:
                create_element(container, "{%s}identifier" % dc,
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]),
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})
            if record_dict["DC2_type"].lower() == "sound":
                create_element(container, "{%s}identifier" % dc, record_dict["c_isReferencedBy_URL"],
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})


def populate_ead_records_element(root_xml, record_dict, verb):
    """
    Depending on the verb (ListIdentifiers, ListRecords, GetRecord)
    Populate the 'root_xml' XML Element with SubElements according to the EAD metadata spec/format
    """
    # TODO support from, to, and set parameters for EAD format
    if verb != "ListIdentifiers":
        record = SubElement(root_xml, "record")
        element = SubElement(record, "header")
    else:
        record = SubElement(root_xml, "header")
        element = record

    create_element(element, "identifier", record_dict["c_signum"])
    create_element(element, "datestamp", record_dict["date_modify"])

    if record_dict["to_europeana"]:
        create_element(element, "setSpec", "SLSeuropeana")
    if record_dict["to_ndb"]:
        create_element(element, "setSpec", "SLSfinna")
    if record_dict["status"] == "deleted":
        element.attrib["status"] = "deleted"
    elif verb == "ListRecords" or verb == "GetRecord":
        namespace_map = root_xml.nsmap.copy()
        ead = namespace_map["ead"]

        element = SubElement(record, "metadata")

        element_ead = SubElement(element, "{%s}ead" % ead)

        header_elem = SubElement(element_ead, "{%s}eadheader" % ead)
        header_elem.attrib["langencoding"] = "iso639-2b"
        header_elem.attrib["countryencoding"] = "iso3166-1"
        header_elem.attrib["dateencoding"] = "iso8601"

        # ead:eadid
        create_element(header_elem, "{%s}eadid" % ead, record_dict["c_signum"])

        desc_elem = SubElement(header_elem, "{%s}filedesc" % ead)

        titles_elem = SubElement(desc_elem, "{%s}titlestmt" % ead)

        # ead:titleproper
        create_element(titles_elem, "{%s}titleproper" % ead, "Databaspost på huvudkatalognivå över {}".format(record_dict["c_signum"]))

        publications_elem = SubElement(desc_elem, "{%s}publicationstmt" % ead)

        # ead:publisher
        create_element(publications_elem, "{%s}publisher" % ead, "Svenska litteratursällskapet i Finland")

        desc_elem = SubElement(header_elem, "{%s}profiledesc" % ead)

        # ead:creation
        create_element(desc_elem, "{%s}creation" % ead,
                       "Beskrivningen tagen ur SLS arkivs databaser, huvudkatalognivån och objektnivån i Arkiva, och exporterat till ead xml.")

        lang_usage_elem = SubElement(desc_elem, "{%s}langusage" % ead)

        # ead:language
        create_element(lang_usage_elem, "{%s}language" % ead, "Svenska",
                       attributes={"langcode": "swe"})

        level = ""
        if str(record_dict["arkivetsTyp"]).lower() == "arkiv":
            level = "fonds"
        elif str(record_dict["arkivetsTyp"]).lower() == "samling":
            level = "collection"
        archdesc_elem = SubElement(element_ead, "{%s}archdesc" % ead,
                                   attrib={"level": level})

        did_elem = SubElement(archdesc_elem, "{%s}did" % ead)

        # ead:head
        create_element(did_elem, "{%s}head" % ead, "Huvudkatalog")

        # ead:unittitle
        create_element(did_elem, "{%s}unittitle" % ead, record_dict["arkivetsNamn"])

        if record_dict["c_tid_arkivetsInnehall"]:
            date_elem = create_element(did_elem, "{%s}unitdate" % ead, record_dict["c_tid_arkivetsInnehall"])
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetsInnehall_maskin"] if record_dict["c_tid_arkivetsInnehall_maskin"] else ""
            date_elem.attrib["label"] = "gransar"
            date_elem.attrib["type"] = "inclusive"
            date_elem.attrib["datechar"] = "creation"

        if record_dict["c_tid_arkivetInsamlat"]:
            date_elem = create_element(did_elem, "{%s}unitdate" % ead, record_dict["c_tid_arkivetInsamlat"])
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetInsamlat_maskin"] if record_dict["c_tid_arkivetInsamlat_maskin"] else ""
            date_elem.attrib["label"] = "insamlingsar"
            date_elem.attrib["type"] = "bulk"
            date_elem.attrib["datechar"] = "accumulation"

        if record_dict["c_tid_arkivetInlamnat"]:
            date_elem = create_element(did_elem, "{%s}unitdate" % ead, record_dict["c_tid_arkivetInlamnat"])
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetInlamnat_maskin"] if record_dict["c_tid_arkivetInlamnat_maskin"] else ""
            date_elem.attrib["label"] = "inlämningsar"
            date_elem.attrib["type"] = "bulk"
            date_elem.attrib["datechar"] = "accumulation"

        # ead:unitid
        create_element(did_elem, "{%s}unitid" % ead, record_dict["c_signum"])

        # ead:origination
        create_element(did_elem, "{%s}origination" % ead, record_dict["projekt"],
                       attributes={"{%s}label" % ead: "collector"})

        # ead:physdesc
        physdesc_elem = SubElement(did_elem, "{%s}physdesc" % ead,
                                   attrib={"label": "Extent"})

        # ead:extent
        if record_dict["omfattning_hyllmeter"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} hyllmeter".format(record_dict["omfattning_hyllmeter"]),
                           attributes={"unit": "running_meters"})
        if record_dict["omfattning_arkivenheter"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} arkivenheter".format(record_dict["omfattning_arkivenheter"]),
                           attributes={"unit": "archival_units"})
        if record_dict["omfattning_sidor"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} sidor".format(record_dict["omfattning_sidor"]),
                           attributes={"unit": "pages"})
        if record_dict["omfattning_filmer"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} filmer".format(record_dict["omfattning_filmer"]),
                           attributes={"unit": "films"})
        if record_dict["omfattning_fotografier"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} fotografier".format(record_dict["omfattning_fotografier"]),
                           attributes={"unit": "photographs"})
        if record_dict["omfattning_ljudband"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} ljudband".format(record_dict["omfattning_ljudband"]),
                           attributes={"unit": "audio_tapes"})
        if record_dict["omfattning_skisser"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} skisser".format(record_dict["omfattning_skisser"]),
                           attributes={"unit": "drawings"})
        if record_dict["omfattning_kartor"]:
            create_element(physdesc_elem, "{%s}extent" % ead,
                           "{} kartor".format(record_dict["omfattning_kartor"]),
                           attributes={"unit": "maps"})

        # ead:langmaterial
        # ead:language
        if record_dict["sprak"]:
            language_header_elem = SubElement(did_elem, "{%s}langmaterial" % ead)
            create_element(language_header_elem, "{%s}language" % ead, record_dict["sprak"],
                           attributes={"langcode": "swe"})

        # ead:repository
        # ead:corpname
        repo_elem = SubElement(did_elem, "{%s}repository" % ead,
                               attrib={"label": "Svenska litteratursällskapet i Finland, {}".format(record_dict["slsArkiv"])})

        create_element(repo_elem, "{%s}corpname" % ead, "SLS")

        # ead:physloc
        create_element(did_elem, "{%s}physloc" % ead, record_dict["slsArkiv"])
        create_element(did_elem, "{%s}physloc" % ead, record_dict["arkivetsPlacering"])

        # ead:controlaccess
        if record_dict["c_listaPersonerRoll_webb"]:
            persons = record_dict["c_listaPersonerRoll_webb"].split("; ")

            control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % ead)
            create_element(control_access_elem, "{%s}head" % ead, "author")

            for person in persons:
                person_elements = person.split(" (")
                if len(person_elements) > 1:
                    create_element(control_access_elem, "{%s}persname" % ead, person_elements[0],
                                   attributes={"role": person_elements[1].replace(")", "")})
                else:
                    create_element(control_access_elem, "{%s}persname" % ead, person_elements[0])

        # ead:controlaccess
        # ead:subject
        if record_dict["amnesord"]:
            persons = record_dict["amnesord"].split("; ")

            control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % ead)
            create_element(control_access_elem, "{%s}head" % ead, "topic_facet")

            for person in persons:
                person_elements = person.split(" (")
                if len(person_elements) > 1:
                    attribs = OrderedDict()
                    attribs["href"] = person_elements[1].replace(")", "")
                    attribs["source"] = "YSO"
                    attribs["lang"] = "swe"
                    create_element(control_access_elem, "{%s}subject" % ead, person_elements[0],
                                   attributes=attribs)
                else:
                    create_element(control_access_elem, "{%s}subject" % ead, person_elements[0],
                                   attributes={"rules": "internal"})

        # ead:controlaccess
        # ead:geogname
        for placelist in [record_dict["c_listaPlatser"], record_dict["c_listaPlatser_fin"]]:
            if placelist:
                control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "geographic_facet")
                create_split_element(control_access_elem, "{%s}geogname" % ead, placelist)

        # ead:bioghist
        if record_dict["c_omArkivbildaren_webb"]:
            bioghist_elem = SubElement(archdesc_elem, "{%s}bioghist" % ead)
            create_split_element(bioghist_elem, "{%s}p" % ead, record_dict["c_omArkivbildaren_webb"], delimiter=";")

        # ead:scopecontent
        scopecontent_elem = SubElement(archdesc_elem, "{%s}scopecontent" % ead)
        create_element(scopecontent_elem, "{%s}head" % ead, "description")
        for text in [record_dict["arkivetsInnehall"], record_dict["anmarkningarExterna"], record_dict["anmarkningarReferens"]]:
            create_element(scopecontent_elem, "{%s}p" % ead, text)

        # ead:accessrestrict
        if record_dict["nyttjanderatt"]:
            accessrestrict_elem = SubElement(archdesc_elem, "{%s}accessrestrict" % ead)
            create_split_element(accessrestrict_elem, "{%s}p" % ead, record_dict["nyttjanderatt"])

        # ead:dsc
        dsc_elem = SubElement(archdesc_elem, "{%s}dsc" % ead,
                              attrib={"type": "combined"})

        # get all items that belong to the collection
        xlink = "http://www.w3.org/1999/xlink"
        namespace_map["xlink"] = xlink
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM intellectualEntities WHERE c_samlingsnummer = '%s'", [record_dict["nummer"]])
            result = cursor.fetchall()
        for row in result:
            c_elem = SubElement(dsc_elem, "{%s}c" % ead,
                                attrib={"level": "item"})

            did_elem = SubElement(c_elem, "{%s}did" % ead)

            # ead:unittitle
            create_element(did_elem, "{%s}unittitle" % ead, row["c_title"])

            # ead:unitdate
            if row["dcterms_created_maskinlasbart"]:
                date_elem = SubElement(did_elem, "{%s}unitdate" % ead,
                                       attrib={"normal": row["dcterms_created_maskinlasbart"]})
                date_elem.text = row["dcterms_created_maskinlasbart"]
                date_elem.attrib["type"] = "bulk"
                date_elem.attrib["datechar"] = "creation"

            # ead:unitid
            create_element(did_elem, "{%s}unitid" % ead, row["finna_unitid"],
                           attributes={"label": "accession_number"})

            # get all URNs for this IE
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM URN WHERE id_IE = '%s'", [row["nummer"]])
                sub_result = cursor.fetchall()

            # ead:unitid
            for sub_row in sub_result:
                create_element(did_elem, "{%s}unitid" % ead, sub_row["URN"],
                               attributes={"label": "PID"})

            # ead:dimensions
            create_element(did_elem, "{%s}dimensions" % ead, row["dc_source_dimensions"])

            # ead:physdesc
            create_element(did_elem, "{%s}physdesc" % ead, row["dc_source2"])

            # ead:language
            if row["dc_language"]:
                langmaterial_elem = SubElement(did_elem, "{%s}langmaterial" % ead)
                create_element(langmaterial_elem, "{%s}language" % ead, row["dc_language"])

            # get all digitalObjects for this IE
            with connection.cursor() as cursor:
                cursor.execute("SELECT nummer, entity_label, entity_order FROM digitalObjects WHERE c_ienummer='%s' ORDER BY entity_order", [row["nummer"]])
                sub_result = cursor.fetchall()

            for sub_row in sub_result:
                # ead:daogrp
                grp_elem = SubElement(did_elem, "{%s}daogrp" % ead)
                if sub_row["entity_label"]:
                    desc_elem = SubElement(grp_elem, "{%s}daodesc" % ead)
                    create_element(desc_elem, "{%s}p" % ead, sub_row["entity_label"])

                # get all derivateObjects that belong to this digitalObject
                with connection.cursor() as cursor:
                    cursor.execute("SELECT derivateObjects.roleTitle, derivateObjects.filePath, digitalObjects.entity_order "
                                   "FROM derivateObjects JOIN digitalObjects ON derivateObjects.c_do = digitalObjects.nummer "
                                   "WHERE c_do='%s' ORDER BY digitalObjects.entity_order", [sub_row["nummer"]])
                    sub_sub_result = cursor.fetchall()

                for sub_sub_row in sub_sub_result:
                    # ead:daoloc
                    loc_elem = create_empty_element(grp_elem, "{%s}daoloc" % ead,
                                                    attributes={"{%s}label" % xlink: sub_sub_row["roleTitle"]},
                                                    namespace_map=namespace_map)
                    if sub_sub_row["roleTitle"] == "Kundkopia":
                        loc_elem.attrib["role"] = "image_full"
                    elif sub_sub_row["roleTitle"] == "Thumbnail":
                        loc_elem.attrib["role"] = "image_thumbnail"
                    elif sub_sub_row["roleTitle"] == "Databasbild":
                        loc_elem.attrib["role"] = "image_reference"
                    elif sub_sub_row["roleTitle"] == "sound_reference":
                        loc_elem.attrib["role"] = "sound_reference"

                    loc_elem.attrib["{%s}href" % xlink] = sub_sub_row["filePath"]

            # ead:daoloc
            if row["c_isReferencedBy_URL"]:
                grp_elem = SubElement(did_elem, "{%s}daogrp" % ead)
                desc_elem = SubElement(grp_elem, "{%s}daodesc" % ead)
                create_element(desc_elem, "{%s}p" % ead, row["c_isReferencedBy_URL"])

                loc_elem = create_empty_element(grp_elem, "{%s}daoloc" % ead,
                                                attributes={"{%s}label" % xlink: "context_www"},
                                                namespace_map=namespace_map)
                loc_elem.attrib["role"] = "url"
                loc_elem.attrib["{%s}href" % xlink] = row["c_isReferencedBy_URL"]

            # ead:scopecontent
            if row["dc_description"]:
                scopecontent_elem = SubElement(c_elem, "{%s}scopecontent" % ead)
                create_element(scopecontent_elem, "{%s}head" % ead, "description")
                create_element(scopecontent_elem, "{%s}p" % ead, row["dc_description"])
                create_element(scopecontent_elem, "{%s}p" % ead, row["dcterms_isReferencedBy"])

            # ead:userestrict
            # ead:accessrestrict
            if row["dc_rights"]:
                for elem_tag in ["{%s}userestrict" % ead, "{%s}accessrestrict" % ead]:
                    restrict_elem = SubElement(c_elem, elem_tag)
                    if row["dc_rights"] == "CC BY 4.0":
                        p_elem = create_element(restrict_elem, "{%s}p" % ead, row["dc_rights"])
                        create_empty_element(p_elem, "{%s}extptr" % ead,
                                             attributes={"href": "https://creativecommons.org/licenses/by/4.0/"})
                    else:
                        create_element(restrict_elem, "{%s}p" % ead, row["dc_rights"],
                                       attributes={"lang": "swe"})

                        create_element(restrict_elem, "{%s}p" % ead, row["rights_fin"],
                                       attributes={"lang": "fin"})

                        create_element(restrict_elem, "{%s}p" % ead, row["rights_eng"],
                                       attributes={"lang": "eng"})

            # ead:controlaccess
            # ead:genreform
            if row["dc_type"] or row["dc_type2"]:
                control_access_elem = SubElement(c_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "format")
                create_split_element(control_access_elem, "{%s}genreform" % ead,
                                     row["dc_type"] if row["dc_type"] else "")

            # ead:controlaccess
            # ead:persname
            if row["dc_creator"]:
                control_access_elem = SubElement(c_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "author")
                create_split_element(control_access_elem, "{%s}persname" % ead, row["dc_creator"],
                                     attributes={"role": "creator"}, delimiter="; ")

            # ead:controlaccess
            # ead:subject
            if row["dc_subject"]:
                persons = row["dc_subject"].split("; ")

                control_access_elem = SubElement(c_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "topic_facet")

                for person in persons:
                    person_elements = person.split(" (")
                    if len(person_elements) > 1:
                        attribs = OrderedDict()
                        attribs["href"] = person_elements[1].replace(")", "")
                        attribs["source"] = "YSO"
                        attribs["lang"] = "swe"
                        create_element(control_access_elem, "{%s}subject" % ead, person_elements[0],
                                       attributes=attribs)
                    else:
                        create_element(control_access_elem, "{%s}subject" % ead, person_elements[0],
                                       attributes={"rules": "internal"})

            # ead:controlaccess
            # ead:geogname
            if row["dcterms_spatial_full"]:
                control_access_elem = SubElement(c_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "geographic_facet")
                create_split_element(control_access_elem, "{%s}geogname" % ead,
                                     row["dcterms_spatial_full"],
                                     attributes={"lang": "swe"})

            # ead:controlaccess
            # ead:geogname
            if row["dcterms_spatial_fin"]:
                control_access_elem = SubElement(c_elem, "{%s}controlaccess" % ead)
                create_element(control_access_elem, "{%s}head" % ead, "geographic_facet")
                create_split_element(control_access_elem, "{%s}geogname" % ead,
                                     row["dcterms_spatial_fin"],
                                     attributes={"lang": "fin"})


def create_error_xml(base_url, verb=None, error_type=u"badVerb", error_text=u"Bad OAI verb"):
    """
    Create an OAI-compliant XML return for an error
    """
    xmlns = "http://www.openarchives.org/OAI/2.0/"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"

    root_element = Element("{%s}OAI-PMH" % xmlns,
                           attrib={"{%s}schemaLocation" % xsi: "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"},
                           nsmap={None: xmlns, "xsi": xsi})

    create_element(root_element, "responseDate", datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"))
    create_element(root_element, "request", base_url, attributes=None if verb is None else {"verb": verb})
    create_element(root_element, "error", error_text, attributes={"code": error_type})

    return tostring(root_element, encoding="UTF-8", pretty_print=True, xml_declaration=True)


def generate_sql_query(valid_params):
    """
    Generate the needed SQL query based on the parameters of the request
    """
    from_sql = ""
    if "from" in valid_params:
        from_sql = " AND GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) >= '{}'".format(valid_params["from"])
    until_sql = ""
    if "until" in valid_params:
        until_sql = " AND GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) <= '{}'".format(valid_params["until"])

    set_sql = ""
    if "set" in valid_params:
        if valid_params["set"] == "SLSeuropeana":
            set_sql = " AND digitalObjects.to_europeana = 'europeana'"
        elif valid_params["set"] == "SLSfinna":
            set_sql = " AND samlingar.to_ndb = 'finna'"

    if valid_params["verb"] == "ListRecords":
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, MAX(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer{}{}{} GROUP BY samlingar.c_signum".format(set_sql, from_sql, until_sql)
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer{}{}{}".format(set_sql, from_sql, until_sql)

    elif valid_params["verb"] == "ListIdentifiers":
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, MAX(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer{}{}{} GROUP BY samlingar.c_signum".format(set_sql, from_sql, until_sql)
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer{}{}{}".format(set_sql, from_sql, until_sql)

    elif valid_params["verb"] == "GetRecord" or (valid_params["verb"] == "ListMetadataFormats" and "identifier" in valid_params):
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, max(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer WHERE samlingar.c_signum='{}' GROUP BY samlingar.c_signum".format(valid_params["identifier"])
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer AND digitalObjects.identifier='{}'".format(valid_params["identifier"])

    else:
        # verb is Identify
        sql_query = "SELECT MIN(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer"

    return sql_query
