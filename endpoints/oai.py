# coding=utf-8
from __future__ import unicode_literals
from collections import OrderedDict
import datetime
from dateutil.parser import parse
from lxml.etree import Element, SubElement, tostring
import os
import pymysql
import traceback
import yaml

valid_OAI_verbs = ["Identify", "ListSets", "ListMetadataFormats", "ListIdentifiers", "ListRecords", "GetRecord"]


def validate_request(request):
    valid_params = {}
    error = None

    for key, value in request.args.iteritems():
        if key not in ["verb", "from", "until", "identifier", "set", "metadataPrefix"]:
            error = ("badArgument", "Unknown argument")

    if "verb" in request.args and request.args["verb"] in valid_OAI_verbs:
        valid_params["verb"] = request.args["verb"]
    else:
        error = ("badVerb", "Bad OAI verb")

    if "from" in request.args:
        try:
            from_date = parse(request.args["from"])
        except Exception:
            error = ("badArgument", "From-date malformed")
        else:
            valid_params["from"] = from_date

    if "until" in request.args:
        try:
            from_date = parse(request.args["until"])
        except Exception:
            error = ("badArgument", "Until-date malformed")
        else:
            valid_params["until"] = from_date

    if "identifier" in request.args:
        valid_params["identifier"] = request.args["identifier"]

    if "set" in request.args:
        if request.args["set"] == "SLSeuropeana":
            valid_params["set"] = request.args["set"]
            valid_params["setName"] = "SLS material till Europeana"
        elif request.args["set"] == "SLSfinna":
            valid_params["set"] = request.args["set"]
            valid_params["setName"] = "SLS material till Finna/NDB"
        else:
            error = ("badArgument", "Unknown set")

    if "metadataPrefix" in request.args:
        prefix = request.args["metadataPrefix"]
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


def get_metadata_from_mysql(valid_params, error):
    config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
    with open(os.path.join(config_dir, "mysql.yml")) as mysql_config_file:
        mysql_config = yaml.load(mysql_config_file)
    result = []
    if error is None:
        try:
            # Use a custom DictCursor that uses collections.OrderedDict
            # This way we maintain column order for each row in the result
            class OrderedDictCursor(pymysql.cursors.DictCursorMixin, pymysql.cursors.Cursor):
                dict_type = OrderedDict

            connection = pymysql.connect(host=mysql_config["address"],
                                         user=mysql_config["username"],
                                         password=mysql_config["password"],
                                         db=mysql_config["database"],
                                         charset="utf8",
                                         cursorclass=OrderedDictCursor)
        except Exception:
            print(traceback.format_exc())
            error = ("internalError", "Could not connect to metadata database")

        else:
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
            finally:
                connection.close()
    return result, error


def get_namespace_map(verb):
    # List of all the namespaces
    xmlns = "http://www.openarchives.org/OAI/2.0/"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"
    oai_dc = "http://www.openarchives.org/OAI/2.0/oai_dc/"
    ead = "http://www.loc.gov/ead"
    dc = "http://purl.org/dc/elements/1.1/"
    dcterms = "http://purl.org/dc/terms/"
    europeana = "http://www.europeana.eu/schemas/ese/"
    if verb in ["ListRecords", "ListIdentifiers", "GetRecord"]:
        # Record elements will use most of the namespaces
        namespace_map = OrderedDict()
        namespace_map[None] = xmlns
        namespace_map["oai_dc"] = oai_dc
        namespace_map["ead"] = ead
        namespace_map["dc"] = dc
        namespace_map["xsi"] = xsi
        namespace_map["dcterms"] = dcterms
        namespace_map["europeana"] = europeana
    else:
        # Identify only needs the None and xsi namespaces
        namespace_map = OrderedDict()
        namespace_map[None] = xmlns
        namespace_map["xsi"] = xsi
    return namespace_map


def create_root_element(verb):
    root_tag = "OAI-PMH"

    namespace_map = get_namespace_map(verb)
    root_attrs = OrderedDict()
    root_attrs["{%s}schemaLocation" % namespace_map["xsi"]] = "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"

    return Element("{%s}%s" % (namespace_map[None], root_tag), root_attrs, nsmap=namespace_map)


def populate_identify_element(base_url, root_xml, earliest_date_stamp):
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
        elem = SubElement(root_xml, key)
        elem.text = value


def populate_listsets_element(root_xml):
    europeana_set_element = SubElement(root_xml, "set")
    europeana_spec = SubElement(europeana_set_element, "setSpec")
    europeana_spec.text = "SLSeuropeana"
    europeana_name = SubElement(europeana_set_element, "setName")
    europeana_name.text = "SLS material till Europeana"

    finna_set_element = SubElement(root_xml, "set")
    finna_spec = SubElement(finna_set_element, "setSpec")
    finna_spec.text = "SLSfinna"
    finna_name = SubElement(finna_set_element, "setName")
    finna_name.text = "SLS material till Finna/NDB"


def populate_listmetadataformats_element(root_xml):
    oai_dc_root = SubElement(root_xml, "metadataFormat")
    oai_dc_prefix = SubElement(oai_dc_root, "metadataPrefix")
    oai_dc_prefix.text = "oai_dc"
    oai_dc_schema = SubElement(oai_dc_root, "schema")
    oai_dc_schema.text = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
    oai_dc_namespace = SubElement(oai_dc_root, "metadataNamespace")
    oai_dc_namespace.text = "http://www.openarchives.org/OAI/2.0/oai_dc/"

    europeana_root = SubElement(root_xml, "metadataFormat")
    europeana_prefix = SubElement(europeana_root, "metadataPrefix")
    europeana_prefix.text = "europeana"
    europeana_schema = SubElement(europeana_root, "schema")
    europeana_schema.text = "http://www.europeana.eu/schemas/ese/ESE-V3.4.xsd"
    europeana_namespace = SubElement(europeana_root, "metadataNamespace")
    europeana_namespace.text = "http://www.europeana.eu/schemas/ese/"

    ead_root = SubElement(root_xml, "metadataFormat")
    ead_prefix = SubElement(ead_root, "metadataPrefix")
    ead_prefix.text = "ead"
    ead_schema = SubElement(ead_root, "schema")
    ead_schema.text = "http://www.loc.gov/ead/ead.xsd"
    ead_namespace = SubElement(ead_root, "metadataNamespace")
    ead_namespace.text = "http://www.loc.gov/ead"


def populate_records_element(root_xml, record_dict, metadata_prefix, verb):
    # TODO don't add an XML tag if it's text is going to be empty-string
    # ListIdentifiers, ListRecords, GetRecord
    if verb != "ListIdentifiers":
        record = SubElement(root_xml, "record")
        element = SubElement(record, "header")
    else:
        record = SubElement(root_xml, "header")
        element = record
    identifier = SubElement(element, "identifier")
    identifier.text = record_dict["identifier"]
    datestamp = SubElement(element, "datestamp")
    datestamp.text = record_dict["date_modified"]
    if record_dict["to_europeana"]:
        set_spec = SubElement(element, "setSpec")
        set_spec.text = "SLSeuropeana"
        # if verb == "GetRecord":
        #     set_name = SubElement(element, "setName")
        #     set_name.text = "SLS material till Europeana"
    if record_dict["to_ndb"]:
        set_spec = SubElement(element, "setSpec")
        set_spec.text = "SLSfinna"
        # if verb == "GetRecord":
        #     set_name = SubElement(element, "setName")
        #     set_name.text = "SLS material till Finna/NDB"
    if record_dict["status"] == "deleted":
        element.attrib["status"] = "deleted"
    elif verb == "ListRecords" or verb == "GetRecord":
        # Add record metadata, lots of it.
        xml = "http://www.w3.org/XML/1998/namespace"
        namespace_map = get_namespace_map(verb)
        namespace_map["xml"] = xml
        element = SubElement(record, "metadata")

        if metadata_prefix == "europeana":
            container = SubElement(element, "{%s}record" % namespace_map["europeana"])
        else:
            container = SubElement(element, "{%s}dc" % namespace_map["oai_dc"])

        if record_dict["dc_title"]:
            title_elem = SubElement(container, "{%s}title" % namespace_map["dc"])
            title_elem.text = record_dict["dc_title"]

        if record_dict["dc_type2"]:
            type_elem = SubElement(container, "{%s}type" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
            type_elem.text = record_dict["dc_type2"]

        if record_dict["DC2_type"].lower() == "sound":
            label_elem = SubElement(container, "{%s}type" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
            label_elem.text = record_dict["entity_label"]

        if record_dict["dc_type2_eng"]:
            type_elem = SubElement(container, "{%s}type" % namespace_map["dc"], attrib={"{%s}lang" % xml: "en"})
            type_elem.text = record_dict["dc_type2_eng"]

        if record_dict["dc_subject"]:
            if "," in record_dict["dc_subject"]:
                for split_subject in record_dict["dc_subject"].split(","):
                    subject_elem = SubElement(container, "{%s}subject" % namespace_map["dc"],attrib={"{%s}lang" % xml: "sv"})
                    subject_elem.text = split_subject.strip()
            else:
                subject_elem = SubElement(container, "{%s}subject" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
                subject_elem.text = record_dict["dc_subject"]

        if record_dict["dc_description"]:
            description_elem = SubElement(container, "{%s}description" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
            description_elem.text = record_dict["dc_description"]

        if record_dict["DC2_type"].lower() == "text":
            description_elem = SubElement(container, "{%s}description" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
            description_elem.text = record_dict["entity_label"]

        if record_dict["dc_source"]:
            source_elem = SubElement(container, "{%s}source" % namespace_map["dc"])
            source_elem.text = record_dict["dc_source"]

        if metadata_prefix == "europeana":
            if record_dict["arkivetsNamn"]:
                is_part_of_elem = SubElement(container, "{%s}isPartOf" % namespace_map["dcterms"])
                is_part_of_elem.text = record_dict["arkivetsNamn"]
            if record_dict["c_signum"]:
                is_part_of_elem = SubElement(container, "{%s}isPartOf" % namespace_map["dcterms"])
                is_part_of_elem.text = record_dict["c_signum"]

            for i in range(1, 5):
                if record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"]:
                    spatial_elem = SubElement(container, "{%s}spatial" % namespace_map["dcterms"], attrib={"{%s}lang" % xml: "sv"})
                    spatial_elem.text = record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"]

            # TODO check the namespace on the attrib for dcterms:created
            if record_dict["dcterms_created_maskinlasbart"]:
                created_elem = SubElement(container, "{%s}created" % namespace_map["dcterms"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
                created_elem.text = record_dict["dcterms_created_maskinlasbart"]

            if record_dict["dcterms_isReferencedBy"]:
                is_referenced_by_elem = SubElement(container, "{%s}isReferencedBy" % namespace_map["dcterms"])
                is_referenced_by_elem.text = record_dict["dcterms_isReferencedBy"]

            if record_dict["dc_identifier"]:
                is_format_of_elem = SubElement(container, "{%s}isFormatOf" % namespace_map["dcterms"])
                is_format_of_elem.text = record_dict["dc_identifier"]

            if record_dict["dc_source_dimensions"] or record_dict["duration"]:
                extent_elem = SubElement(container, "{%s}extent" % namespace_map["dcterms"])
                if record_dict["dc_source_dimensions"]:
                    extent_elem.text = record_dict["dc_source_dimentions"]
                else:
                    extent_elem.text = record_dict["duration"]

            if record_dict["dc_source2"]:
                medium_elem = SubElement(container, "{%s}medium" % namespace_map["dcterms"])
                medium_elem.text = record_dict["dc_source2"]
        else:
            if record_dict["arkivetsNamn"]:
                relation_elem = SubElement(container, "{%s}relation" % namespace_map["dc"])
                relation_elem.text = record_dict["arkivetsNamn"]
            if record_dict["c_signum"]:
                relation_elem = SubElement(container, "{%s}relation" % namespace_map["dc"])
                relation_elem.text = record_dict["c_signum"]

            for i in range(1, 5):
                if record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"]:
                    coverage_elem = SubElement(container, "{%s}coverage" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
                    coverage_elem.text = record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"]

            if record_dict["dcterms_created_maskinlasbart"]:
                date_elem = SubElement(container, "{%s}date" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
                date_elem.text = record_dict["dcterms_created_maskinlasbart"]

            if record_dict["dcterms_isReferencedBy"]:
                relation_elem = SubElement(container, "{%s}relation" % namespace_map["dc"])
                relation_elem.text = record_dict["dcterms_isReferencedBy"]

            if record_dict["dc_identifier"]:
                relation_elem = SubElement(container, "{%s}relation" % namespace_map["dc"])
                relation_elem.text = record_dict["dc_identifier"]

            if record_dict["dc_source_dimensions"] or record_dict["duration"]:
                format_elem = SubElement(container, "{%s}format" % namespace_map["dc"])
                if record_dict["dc_source_dimensions"]:
                    format_elem.text = record_dict["dc_source_dimensions"]
                else:
                    format_elem.text = record_dict["duration"]

            if record_dict["dc_source2"]:
                format_elem = SubElement(container, "{%s}format" % namespace_map["dc"])
                format_elem.text = record_dict["dc_source2"]

        if record_dict["filetype_MIME"]:
            format_elem = SubElement(container, "{%s}format" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:IMT"})
            format_elem.text = record_dict["filetype_MIME"]

        if record_dict["dc_creator"]:
            creator_elem = SubElement(container, "{%s}creator" % namespace_map["dc"])
            creator_elem.text = record_dict["dc_creator"]

        # join together the two publisher fields
        publisher = ""
        if record_dict["dc_publisher"]:
            publisher += record_dict["dc_publisher"]
        if record_dict["dc_publisher2"]:
            if len(publisher) > 0:
                publisher += ", "
            publisher += record_dict["dc_publisher2"]
        if publisher:
            publisher_elem = SubElement(container, "{%s}publisher" % namespace_map["dc"])
            publisher_elem.text = publisher

        if record_dict["dc_rights"]:
            rights_elem = SubElement(container, "{%s}rights" % namespace_map["dc"], attrib={"{%s}lang" % xml: "sv"})
            rights_elem.text = record_dict["dc_rights"]

        if record_dict["DCterms_issued"]:
            if metadata_prefix == "europeana":
                issued_elem = SubElement(container, "{%s}issued" % namespace_map["dcterms"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
            else:
                issued_elem = SubElement(container, "{%s}date" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
            issued_elem.text = record_dict["DCterms_issued"]

        if record_dict["identifier"]:
            identifier_elem = SubElement(container, "{%s}identifier" % namespace_map["dc"])
            identifier_elem.text = record_dict["identifier"]

        if record_dict["dc_language"]:
            language_elem = SubElement(container, "{%s}language" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:ISO639-2"})
            language_elem.text = record_dict["dc_language"]

        if metadata_prefix == "europeana":
            # TODO check filepath base against new API (api.sls.fi/images)
            if record_dict["derivate_filepath"]:
                filepath_elem = SubElement(container, "{%s}object" % namespace_map["europeana"])
                filepath_elem.text = "http://api.sls.fi/images/{}".format(record_dict["derivate_filepath"])

            provider_elem = SubElement(container, "{%s}provider" % namespace_map["europeana"])
            provider_elem.text = "National Formula agreement"

            if record_dict["ESE_type"]:
                type_elem = SubElement(container, "{%s}type" % namespace_map["europeana"])
                type_elem.text = record_dict["ESE_type"]

            if record_dict["europeanaRights"]:
                rights_elem = SubElement(container, "{%s}rights" % namespace_map["europeana"])
                rights_elem.text = record_dict["europeanaRights"]

            dataprovider_elem = SubElement(container, "{%s}dataProvider" % namespace_map["europeana"])
            dataprovider_elem.text = "Svenska litteratursällskapet i Finland"

            # TODO check filepath base against new API (api.sls.fi/images)
            if record_dict["derivate_filepath"]:
                shownby_elem = SubElement(container, "{%s}isShownBy" % namespace_map["europeana"])
                shownby_elem.text = "http://api.sls.fi/images/{}".format(record_dict["derivate_filepath"])

            if record_dict["DC2_type"].lower() == "sound":
                shownat_elem = SubElement(container, "{%s}isShownAt" % namespace_map["europeana"])
                shownat_elem.text = record_dict["c_isReferencedBy_URL"]
        else:
            # TODO check namespaces on the attribs in this chunk
            # TODO check filepath base against new API (api.sls.fi/images)
            if record_dict["derivate_filepath"]:
                filepath_elem = SubElement(container, "{%s}identifier" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})
                filepath_elem.text = "http://api.sls.fi/images/{}".format(record_dict["derivate_filepath"])

            publisher_elem = SubElement(container, "{%s}publisher" % namespace_map["dc"])
            publisher_elem.text = "National Formula agreement"

            if record_dict["ESE_type"]:
                type_elem = SubElement(container, "{%s}type" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:DCMItype"})
                type_elem.text = record_dict["ESE_type"]

            if record_dict["europeanaRights"]:
                rights_elem = SubElement(container, "{%s}rights" % namespace_map["dc"])
                rights_elem.text = record_dict["europeanaRights"]

            publisher_elem = SubElement(container, "{%s}publisher" % namespace_map["dc"])
            publisher_elem.text = "Svenska litteratursällskapet i Finland"

            if record_dict["derivate_filepath"]:
                identifier_elem = SubElement(container, "{%s}identifier" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})
                identifier_elem.text = "http://api.sls.fi/images/{}".format(record_dict["derivate_filepath"])

            if record_dict["DC2_type"].lower() == "sound":
                identifier_elem = SubElement(container, "{%s}identifier" % namespace_map["dc"], attrib={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})
                identifier_elem.text = record_dict["c_isReferencedBy_URL"]


def populate_ead_records_element(root_xml, record_dict, metadata_prefix, verb):
    # ListIdentifiers, ListRecords, GetRecord - following EAD metadata format
    if verb != "ListIdentifiers":
        record = SubElement(root_xml, "record")
        element = SubElement(record, "header")
    else:
        record = SubElement(root_xml, "header")
        element = record
    identifier = SubElement(element, "identifier")
    identifier.text = record_dict["c_signum"]
    datestamp = SubElement(element, "datestamp")
    datestamp.text = record_dict["date_modify"].strftime("%Y-%m-%d")
    if record_dict["to_europeana"]:
        set_spec = SubElement(element, "setSpec")
        set_spec.text = "SLSeuropeana"
        if verb == "GetRecord":
            set_name = SubElement(element, "setName")
            set_name.text = "SLS material till Europeana"
    if record_dict["to_ndb"]:
        set_spec = SubElement(element, "setSpec")
        set_spec.text = "SLSfinna"
        if verb == "GetRecord":
            set_name = SubElement(element, "setName")
            set_name.text = "SLS material till Finna/NDB"
    if record_dict["status"] == "deleted":
        element.attrib["status"] = "deleted"
    elif verb == "ListRecords" or verb == "GetRecord":
        namespace_map = get_namespace_map(verb)
        temp_element = SubElement(root_xml, "NotYetImplemented")
        temp_element.text = "This function not yet implemented!"
        # TODO port from functions.php


def create_error_xml(base_url, verb=None, error_type=u"badVerb", error_text=u"Bad OAI verb"):
    xmlns = "http://www.openarchives.org/OAI/2.0/"
    xsi = "http://www.w3.org/2001/XMLSchema-instance"

    root_element = Element("{%s}OAI-PMH" % xmlns,
                           attrib={"{%s}schemaLocation" % xsi: "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"},
                           nsmap={None: xmlns, "xsi": xsi})

    response_date_element = SubElement(root_element, "responseDate")
    response_date_element.text = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    request_element = SubElement(root_element, "request", attrib=None if verb is None else {"verb": verb})
    request_element.text = base_url

    error_element = SubElement(root_element, "error", attrib={"code": error_type})
    error_element.text = error_text

    return tostring(root_element, encoding="utf-8", pretty_print=True)


def generate_sql_query(valid_params):
    from_sql = ""
    if "from" in valid_params:
        from_sql = " AND GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) >= '{}'".format(
            valid_params["from"])
    until_sql = ""
    if "until" in valid_params:
        until_sql = " AND GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) <= '{}'".format(
            valid_params["until"])

    set_sql = ""
    if "set" in valid_params:
        if valid_params["set"] == "SLSeuropeana":
            set_sql = " AND digitalObjects.to_europeana = 'europeana'"
        elif valid_params["set"] == "SLSfinna":
            set_sql = " AND samlingar.to_ndb = 'finna'"

    if valid_params["verb"] == "ListRecords":
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, MAX(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer{}{}{} GROUP BY samlingar.c_signum".format(
                set_sql, from_sql, until_sql)
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer{}{}{}".format(
                set_sql, from_sql, until_sql)
    elif valid_params["verb"] == "ListIdentifiers":
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, MAX(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer{}{}{} GROUP BY samlingar.c_signum".format(
                set_sql, from_sql, until_sql)
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer{}{}{}".format(
                set_sql, from_sql, until_sql)
    elif valid_params["verb"] == "GetRecord" or (
            valid_params["verb"] == "ListMetadataFormats" and "identifier" in valid_params):
        if valid_params["metadataPrefix"] == "ead":
            sql_query = "SELECT DISTINCT samlingar.*, max(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date_modified FROM samlingar LEFT JOIN intellectualEntities ON intellectualEntities.c_samlingsnummer = samlingar.nummer LEFT JOIN digitalObjects ON digitalObjects.c_ienummer = intellectualEntities.nummer WHERE samlingar.c_signum='{}' GROUP BY samlingar.c_signum".format(
                valid_params["identifier"])
        else:
            sql_query = "SELECT *, GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify) AS date_modified FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer AND digitalObjects.identifier='{}'".format(
                valid_params["identifier"])
    else:
        # verb is Identify
        sql_query = "SELECT MIN(GREATEST(digitalObjects.date_create, intellectualEntities.date_create, samlingar.date_create)) AS date FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer"

    return sql_query
