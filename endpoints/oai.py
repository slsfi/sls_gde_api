from collections import OrderedDict
import datetime
from dateutil.parser import parse
from lxml.etree import Element, SubElement, tostring
import os
import pymysql
import traceback
import yaml


def validate_request(request):
    valid_params = {}
    error = None
    verbs = ["Identify", "ListRecords", "ListIdentifiers", "ListMetadataFormats", "ListSets", "GetRecord"]

    for key, value in request.args.iteritems():
        if key not in ["verb", "from", "until", "identifier", "set", "metadataPrefix"]:
            error = ("badArgument", "Unknown argument")

    if "verb" in request.args and request.args["verb"] in verbs:
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


def create_root_element(verb):
    root_tag = "OAI-PMH"
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
        root_attrs = OrderedDict()
        root_attrs[
            "{%s}schemaLocation" % xsi] = "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
    else:
        # Identify only needs the None and xsi namespaces
        namespace_map = OrderedDict()
        namespace_map[None] = xmlns
        namespace_map["xsi"] = xsi
        root_attrs = OrderedDict()
        root_attrs[
            "{%s}schemaLocation" % xsi] = "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"

    return Element("{%s}%s" % (xmlns, root_tag), root_attrs, nsmap=namespace_map)


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
