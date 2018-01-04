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
config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with open(os.path.join(config_dir, "mysql.yml")) as mysql_config_file:
    mysql_config = yaml.load(mysql_config_file)
# accessfile_API_endpoint = "http://api.sls.fi/accessfiles/"
accessfile_API_endpoint = "http://www.sls.fi/databasen/"
# TODO get endpoint programmatically


def create_element(parent, tag_name, contents, attributes=None, namespace_map=None):
    if contents:
        new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
        new_elem.text = contents.strftime("%Y-%m-%d") if isinstance(contents, (datetime.date, datetime.datetime)) else contents
        return new_elem


def create_empty_element(parent, tag_name, attributes=None, namespace_map=None):
    new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
    new_elem.text = ""
    return new_elem


def create_split_element(parent, tag_name, contents, attributes=None, namespace_map=None, delimiter=", "):
    if contents:
        if delimiter in contents:
            split_contents = contents.split(delimiter)
            for content in split_contents:
                new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
                new_elem.text = content
        else:
            new_elem = SubElement(parent, tag_name, attrib=attributes, nsmap=namespace_map)
            new_elem.text = contents


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
            valid_params["from"] = from_date.strftime("%Y-%m-%d")

    if "until" in request.args:
        try:
            until_date = parse(request.args["until"])
        except Exception:
            error = ("badArgument", "Until-date malformed")
        else:
            valid_params["until"] = until_date.strftime("%Y-%m-%d")

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
    create_element(europeana_set_element, "setSpec", "SLSeuropeana")
    create_element(europeana_set_element, "setName", "SLS material till Europeana")

    finna_set_element = SubElement(root_xml, "set")
    create_element(finna_set_element, "setSpec", "SLSfinna")
    create_element(finna_set_element, "setName", "SLS material till Finna/NDB")


def populate_listmetadataformats_element(root_xml):
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
    # ListIdentifiers, ListRecords, GetRecord
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
        namespace_map = get_namespace_map(verb)
        xml = "http://www.w3.org/XML/1998/namespace"
        namespace_map["xml"] = xml
        element = SubElement(record, "metadata")

        if metadata_prefix == "europeana":
            container = SubElement(element, "{%s}record" % namespace_map["europeana"])
        else:
            container = SubElement(element, "{%s}dc" % namespace_map["oai_dc"])

        # dc:title
        create_element(container, "{%s}title" % namespace_map["dc"], record_dict["dc_title"])

        # dc:type
        create_element(container, "{%s}type" % namespace_map["dc"], record_dict["dc_type2"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:type
        if record_dict["DC2_type"] and record_dict["DC2_type"].lower() == "sound":
            create_element(container, "{%s}type" % namespace_map["dc"], record_dict["entity_label"],
                           attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:type
        create_element(container, "{%s}type" % namespace_map["dc"], record_dict["dc_type2_eng"],
                       attributes={"{%s}lang" % xml: "en"}, namespace_map=namespace_map)

        # dc:subject
        create_split_element(container, "{%s}subject" % namespace_map["dc"], record_dict["dc_subject"],
                             attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:description
        create_element(container, "{%s}description" % namespace_map["dc"], record_dict["dc_description"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:description
        if record_dict["DC2_type"] and record_dict["DC2_type"].lower() == "text":
            create_element(container, "{%s}description" % namespace_map["dc"], record_dict["entity_label"],
                           attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dc:source
        create_element(container, "{%s}source" % namespace_map["dc"], record_dict["dc_source"])

        if metadata_prefix == "europeana":
            # dcterms:isPartOf
            create_element(container, "{%s}isPartOf" % namespace_map["dcterms"], record_dict["arkivetsNamn"])
            create_element(container, "{%s}isPartOf" % namespace_map["dcterms"], record_dict["c_signum"])

            # dcterms:spatial
            for i in range(1, 5):
                create_split_element(container, "{%s}spatial" % namespace_map["dcterms"], record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"],
                                     attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

            # dctrems:created
            create_element(container, "{%s}created" % namespace_map["dcterms"], record_dict["dcterms_created_maskinlasbart"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

            # dcterms:isReferencedBy
            create_element(container, "{%s}isReferencedBy" % namespace_map["dcterms"], record_dict["dcterms_isReferencedBy"])

            # dcterms:isFormatOf
            create_element(container, "{%s}isFormatOf" % namespace_map["dcterms"], record_dict["dc_identifier"])

            # dcterms:extent
            create_element(container, "{%s}extent" % namespace_map["dcterms"],
                           record_dict["dc_source_dimensions"] if record_dict["dc_source_dimensions"] else record_dict["duration"])

            # dcterms:medium
            create_element(container, "{%s}medium" % namespace_map["dcterms"], record_dict["dc_source2"])

        else:
            # dc:relation
            create_element(container, "{%s}relation" % namespace_map["dc"], record_dict["arkivetsNamn"])
            create_element(container, "{%s}relation" % namespace_map["dc"], record_dict["c_signum"])

            # dc:coverage
            for i in range(1, 5):
                create_split_element(container, "{%s}coverage" % namespace_map["dc"],
                                     record_dict["dcterms_spatial{}".format(i) if i != 1 else "dcterms_spatial"],
                                     attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

            # dc:date
            create_element(container, "{%s}date" % namespace_map["dc"], record_dict["dcterms_created_maskinlasbart"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

            # dc:relation
            create_element(container, "{%s}relation" % namespace_map["dc"], record_dict["dcterms_isReferencedBy"])
            create_element(container, "{%s}relation" % namespace_map["dc"], record_dict["dc_identifier"])

            # dc:format
            create_element(container, "{%s}format" % namespace_map["dc"],
                           record_dict["dc_source_dimensions"] if record_dict["dc_source_dimensions"] else record_dict["duration"])

            create_element(container, "{%s}format" % namespace_map["dc"], record_dict["dc_source2"])

        # dc:format
        create_element(container, "{%s}format" % namespace_map["dc"], record_dict["filetype_MIME"],
                       attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:IMT"})

        # dc:creator
        create_element(container, "{%s}creator" % namespace_map["dc"], record_dict["dc_creator"])

        # dc:publisher
        # join together the two publisher fields
        publisher = ""
        if record_dict["dc_publisher"]:
            publisher += record_dict["dc_publisher"]
        if record_dict["dc_publisher2"]:
            if len(publisher) > 0:
                publisher += ", "
            publisher += record_dict["dc_publisher2"]
        create_element(container, "{%s}publisher" % namespace_map["dc"], publisher)

        # dc:rights
        create_element(container, "{%s}rights" % namespace_map["dc"], record_dict["dc_rights"],
                       attributes={"{%s}lang" % xml: "sv"}, namespace_map=namespace_map)

        # dcterms:issued / dc:date
        if metadata_prefix == "europeana":
            create_element(container, "{%s}issued" % namespace_map["dcterms"], record_dict["DCterms_issued"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})
        else:
            create_element(container, "{%s}date" % namespace_map["dc"], record_dict["DCterms_issued"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:W3CDTF"})

        # dc:identifier
        create_element(container, "{%s}identifier" % namespace_map["dc"], record_dict["identifier"])

        # dc:language
        create_element(container, "{%s}language" % namespace_map["dc"], record_dict["dc_language"],
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
                create_element(container, "{%s}identifier" % namespace_map["dc"],
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]),
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})

            # dc:publisher
            create_element(container, "{%s}publisher" % namespace_map["dc"], "National Formula agreement")

            # dc:type
            create_element(container, "{%s}type" % namespace_map["dc"], record_dict["ESE_type"],
                           attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:DCMItype"})

            # dc:rights
            create_element(container, "{%s}rights" % namespace_map["dc"], record_dict["europeanaRights"])

            # dc:publisher
            create_element(container, "{%s}publisher" % namespace_map["dc"], "Svenska litteratursällskapet i Finland")

            # dc:identifier
            if record_dict["derivate_filepath"]:
                create_element(container, "{%s}identifier" % namespace_map["dc"],
                               "{}{}".format(accessfile_API_endpoint, record_dict["derivate_filepath"]),
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})
            if record_dict["DC2_type"].lower() == "sound":
                create_element(container, "{%s}identifier" % namespace_map["dc"], record_dict["c_isReferencedBy_URL"],
                               attributes={"{%s}type" % namespace_map["xsi"]: "dcterms:URI"})


def populate_ead_records_element(root_xml, record_dict, verb):
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
        namespace_map = get_namespace_map(verb)

        element = SubElement(record, "metadata")

        element_ead = SubElement(element, "{%s}ead" % namespace_map["ead"])

        header_elem = SubElement(element_ead, "{%s}eadheader" % namespace_map["ead"],
                                 attrib=OrderedDict())
        header_elem.attrib["langencoding"] = "iso639-2b"
        header_elem.attrib["countryencoding"] = "iso3166-1"
        header_elem.attrib["dateencoding"] = "iso8601"

        id_elem = SubElement(header_elem, "{%s}eadid" % namespace_map["ead"])
        id_elem.text = record_dict["c_signum"]

        desc_elem = SubElement(header_elem, "{%s}filedesc" % namespace_map["ead"])

        titles_elem = SubElement(desc_elem, "{%s}titlestmt" % namespace_map["ead"])

        title_proper_elem = SubElement(titles_elem, "{%s}titleproper" % namespace_map["ead"])
        title_proper_elem.text = "Databaspost på huvudkatalognivå över {}".format(record_dict["c_signum"])

        publications_elem = SubElement(desc_elem, "{%s}publicationstmt" % namespace_map["ead"])

        publisher_elem = SubElement(publications_elem, "{%s}publisher" % namespace_map["ead"])
        publisher_elem.text = "Svenska litteratursällskapet i Finland"

        desc_elem = SubElement(header_elem, "{%s}profiledesc" % namespace_map["ead"])

        creation_elem = SubElement(desc_elem, "{%s}creation" % namespace_map["ead"])
        creation_elem.text = "Beskrivningen tagen ur SLS arkivs databaser, huvudkatalognivån och objektnivån i Arkiva, och exporterat till ead xml."

        lang_usage_elem = SubElement(desc_elem, "{%s}langusage" % namespace_map["ead"])

        language_elem = SubElement(lang_usage_elem, "{%s}language" % namespace_map["ead"],
                                   attrib={"langcode": "swe"})
        language_elem.text = "Svenska"

        level = ""
        if str(record_dict["arkivetsTyp"]).lower() == "arkiv":
            level = "fonds"
        elif str(record_dict["arkivetsTyp"]).lower() == "samling":
            level = "collection"
        archdesc_elem = SubElement(element_ead, "{%s}archdesc" % namespace_map["ead"],
                                   attrib={"level": level})

        did_elem = SubElement(archdesc_elem, "{%s}did" % namespace_map["ead"])

        head_elem = SubElement(did_elem, "{%s}head" % namespace_map["ead"])
        head_elem.text = "Huvudkatalog"

        if record_dict["arkivetsNamn"]:
            title_elem = SubElement(did_elem, "{%s}unittitle" % namespace_map["ead"])
            title_elem.text = record_dict["arkivetsNamn"]

        if record_dict["c_tid_arkivetsInnehall"]:
            date_elem = SubElement(did_elem, "{%s}unitdate" % namespace_map["ead"])
            date_elem.text = record_dict["c_tid_arkivetsInnehall"]
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetsInnehall_maskin"] if record_dict["c_tid_arkivetsInnehall_maskin"] else ""
            date_elem.attrib["label"] = "gransar"
            date_elem.attrib["type"] = "inclusive"
            date_elem.attrib["datechar"] = "creation"

        if record_dict["c_tid_arkivetInsamlat"]:
            date_elem = SubElement(did_elem, "{%s}unitdate" % namespace_map["ead"])
            date_elem.text = record_dict["c_tid_arkivetInsamlat"]
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetInsamlat_maskin"] if record_dict["c_tid_arkivetInsamlat_maskin"] else ""
            date_elem.attrib["label"] = "insamlingsar"
            date_elem.attrib["type"] = "bulk"
            date_elem.attrib["datechar"] = "accumulation"

        if record_dict["c_tid_arkivetInlamnat"]:
            date_elem = SubElement(did_elem, "{%s}unitdate" % namespace_map["ead"])
            date_elem.text = record_dict["c_tid_arkivetInlamnat"]
            date_elem.attrib["normal"] = record_dict["c_tid_arkivetInlamnat_maskin"] if record_dict["c_tid_arkivetInlamnat_maskin"] else ""
            date_elem.attrib["label"] = "inlämningsar"
            date_elem.attrib["type"] = "bulk"
            date_elem.attrib["datechar"] = "accumulation"

        id_elem = SubElement(did_elem, "{%s}unitid" % namespace_map["ead"])
        id_elem.text = record_dict["c_signum"]

        if record_dict["projekt"]:
            origination_elem = SubElement(did_elem, "{%s}origination" % namespace_map["ead"],
                                          attrib={"{%s}label" % namespace_map["ead"]: "collector"})
            origination_elem.text = record_dict["projekt"]

        physdesc_elem = SubElement(did_elem, "{%s}physdesc" % namespace_map["ead"],
                                   attrib={"label": "Extent"})
        if record_dict["omfattning_hyllmeter"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "running_meters"})
            extent.text = "{} hyllmeter".format(record_dict["omfattning_hyllmeter"])
        if record_dict["omfattning_arkivenheter"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "archival_units"})
            extent.text = "{} arkivenheter".format(record_dict["omfattning_arkivenheter"])
        if record_dict["omfattning_sidor"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "pages"})
            extent.text = "{} sidor".format(record_dict["omfattning_sidor"])
        if record_dict["omfattning_filmer"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "films"})
            extent.text = "{} filmer".format(record_dict["omfattning_filmer"])
        if record_dict["omfattning_fotografier"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "photographs"})
            extent.text = "{} fotografier".format(record_dict["omfattning_fotografier"])
        if record_dict["omfattning_ljudband"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "audio_tapes"})
            extent.text = "{} ljudband".format(record_dict["omfattning_ljudband"])
        if record_dict["omfattning_skisser"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "drawings"})
            extent.text = "{} skisser".format(record_dict["omfattning_skisser"])
        if record_dict["omfattning_kartor"]:
            extent = SubElement(physdesc_elem, "{%s}extent" % namespace_map["ead"], attrib={"unit": "maps"})
            extent.text = "{} kartor".format(record_dict["omfattning_kartor"])

        if record_dict["sprak"]:
            language_header_elem = SubElement(did_elem, "{%s}langmaterial" % namespace_map["ead"])
            language_elem = SubElement(language_header_elem, "{%s}language" % namespace_map["ead"], attrib={"langcode": "swe"})
            language_elem.text = record_dict["sprak"]

        repo_elem = SubElement(did_elem, "{%s}repository" % namespace_map["ead"],
                               attrib={"label": "Svenska litteratursällskapet i Finland, {}".format(record_dict["slsArkiv"])})
        corp_elem = SubElement(repo_elem, "{%s}corpname" % namespace_map["ead"])
        corp_elem.text = "SLS"

        if record_dict["slsArkiv"]:
            physloc_elem = SubElement(did_elem, "{%s}physloc" % namespace_map["ead"])
            physloc_elem.text = record_dict["slsArkiv"]
        if record_dict["arkivetsPlacering"]:
            physloc_elem = SubElement(did_elem, "{%s}physloc" % namespace_map["ead"])
            physloc_elem.text = record_dict["arkivetsPlacering"]

        if record_dict["c_listaPersonerRoll_webb"]:
            persons = record_dict["c_listaPersonerRoll_webb"].split("; ")

            control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % namespace_map["ead"])
            head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
            head_elem.text = "author"

            for person in persons:
                person_elements = person.split(" (")
                if len(person_elements) > 1:
                    persname_elem = SubElement(control_access_elem, "{%s}persname" % namespace_map["ead"],
                                               attrib={"role": person_elements[1].replace(")", "")})
                    persname_elem.text = person_elements[0]
                else:
                    persname_elem = SubElement(control_access_elem, "{%s}persname" % namespace_map["ead"])
                    persname_elem.text = person_elements[0]

        if record_dict["amnesord"]:
            persons = record_dict["amnesord"].split("; ")

            control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % namespace_map["ead"])
            head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
            head_elem.text = "topic_facet"

            for person in persons:
                person_elements = person.split(" (")
                if len(person_elements) > 1:
                    subject_elem = SubElement(control_access_elem, "{%s}subject" % namespace_map["ead"],
                                              attrib={"href": person_elements[1].replace(")", "")})
                    subject_elem.attrib["source"] = "YSO"
                    subject_elem.attrib["lang"] = "swe"
                    subject_elem.text = person_elements[0]
                else:
                    subject_elem = SubElement(control_access_elem, "{%s}subject" % namespace_map["ead"],
                                              attrib={"rules": "internal"})
                    subject_elem.text = person_elements[0]

        for placelist in [record_dict["c_listaPlatser"], record_dict["c_listaPlatser_fin"]]:
            if placelist:
                control_access_elem = SubElement(archdesc_elem, "{%s}controlaccess" % namespace_map["ead"])
                head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                head_elem.text = "geographic_facet"
                if ", " in placelist:
                    for split_place in placelist.split(", "):
                        if split_place:
                            geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"])
                            geogname_elem.text = split_place
                else:
                    geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"])
                    geogname_elem.text = placelist

        if record_dict["c_omArkivbildaren_webb"]:
            about_persons = record_dict["c_omArkivbildaren_webb"].split(";")
            bioghist_elem = SubElement(archdesc_elem, "{%s}bioghist" % namespace_map["ead"])
            for person in about_persons:
                p_elem = SubElement(bioghist_elem, "{%s}p" % namespace_map["ead"])
                p_elem.text = person

        scopecontent_elem = SubElement(archdesc_elem, "{%s}scopecontent" % namespace_map["ead"])
        head_elem = SubElement(scopecontent_elem, "{%s}head" % namespace_map["ead"])
        head_elem.text = "description"
        for text in [record_dict["arkivetsInnehall"], record_dict["anmarkningarExterna"], record_dict["anmarkningarReferens"]]:
            if text:
                p_elem = SubElement(scopecontent_elem, "{%s}p" % namespace_map["ead"])
                p_elem.text = text

        if record_dict["nyttjanderatt"]:
            accessrestrict_elem = SubElement(archdesc_elem, "{%s}accessrestrict" % namespace_map["ead"])
            if ", " in record_dict["nyttjanderatt"]:
                split_text = record_dict["nyttjanderatt"].split(", ")
                for text in split_text:
                    if text:
                        p_elem = SubElement(accessrestrict_elem, "{%s}p" % namespace_map["ead"])
                        p_elem.text = text
            else:
                p_elem = SubElement(accessrestrict_elem, "{%s}p" % namespace_map["ead"])
                p_elem.text = record_dict["nyttjanderatt"]

        dsc_elem = SubElement(archdesc_elem, "{%s}dsc" % namespace_map["ead"],
                              attrib={"type": "combined"})

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
            print traceback.format_exc()

        else:
            xlink = "http://www.w3.org/1999/xlink"
            namespace_map["xlink"] = xlink
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM intellectualEntities WHERE c_samlingsnummer = %s", [record_dict["nummer"]])
                result = cursor.fetchall()

            for row in result:
                c_elem = SubElement(dsc_elem, "{%s}c" % namespace_map["ead"],
                                    attrib={"level": "item"})

                did_elem = SubElement(c_elem, "{%s}did" % namespace_map["ead"])
                if row["c_title"]:
                    title_elem = SubElement(did_elem, "{%s}unittitle" % namespace_map["ead"])
                    title_elem.text = row["c_title"]

                if row["dcterms_created_maskinlasbart"]:
                    date_elem = SubElement(did_elem, "{%s}unitdate" % namespace_map["ead"],
                                           attrib={"normal": row["dcterms_created_maskinlasbart"]})
                    date_elem.text = row["dcterms_created_maskinlasbart"]
                    date_elem.attrib["type"] = "bulk"
                    date_elem.attrib["datechar"] = "creation"

                if row["finna_unitid"]:
                    unitid_elem = SubElement(did_elem, "{%s}unitid" % namespace_map["ead"],
                                             attrib={"label": "accession_number"})
                    unitid_elem.text = row["finna_unitid"]

                with connection.cursor() as cursor:
                    cursor.execute("SELECT * FROM URN WHERE id_IE = %s", [row["nummer"]])
                    sub_result = cursor.fetchall()

                for sub_row in sub_result:
                    unitid_elem = SubElement(did_elem, "{%s}unitid" % namespace_map["ead"],
                                             attrib={"label": "PID"})
                    unitid_elem.text = sub_row["URN"]

                if row["dc_source_dimensions"]:
                    dimensions_elem = SubElement(did_elem, "{%s}dimensions" % namespace_map["ead"])
                    dimensions_elem.text = row["dc_source_dimensions"]

                if row["dc_source2"]:
                    physdesc_elem = SubElement(did_elem, "{%s}physdesc" % namespace_map["ead"])
                    physdesc_elem.text = row["dc_source2"]

                if row["dc_language"]:
                    langmaterial_elem = SubElement(did_elem, "{%s}langmaterial" % namespace_map["ead"])
                    language_elem = SubElement(langmaterial_elem, "{%s}language" % namespace_map["ead"])
                    language_elem.text = row["dc_language"]

                with connection.cursor() as cursor:
                    cursor.execute("SELECT nummer, entity_label, entity_order FROM digitalObjects WHERE c_ienummer=%s ORDER BY entity_order", [row["nummer"]])
                    sub_result = cursor.fetchall()

                for sub_row in sub_result:
                    grp_elem = SubElement(did_elem, "{%s}daogrp" % namespace_map["ead"])
                    if sub_row["entity_label"]:
                        desc_elem = SubElement(grp_elem, "{%s}daodesc" % namespace_map["ead"])
                        p_elem = SubElement(desc_elem, "{%s}p" % namespace_map["ead"])
                        p_elem.text = sub_row["entity_label"]

                    with connection.cursor() as cursor:
                        cursor.execute("SELECT derivateObjects.roleTitle, derivateObjects.filePath, digitalObjects.entity_order "
                                       "FROM derivateObjects JOIN digitalObjects ON derivateObjects.c_do = digitalObjects.nummer "
                                       "WHERE c_do=%s ORDER BY digitalObjects.entity_order", [sub_row["nummer"]])
                        sub_sub_result = cursor.fetchall()

                    for sub_sub_row in sub_sub_result:
                        loc_elem = SubElement(grp_elem, "{%s}daoloc" % namespace_map["ead"],
                                              attrib={"{%s}label" % xlink: sub_sub_row["roleTitle"]}, nsmap=namespace_map)
                        loc_elem.text = ""
                        if sub_sub_row["roleTitle"] == "Kundkopia":
                            loc_elem.attrib["role"] = "image_full"
                        elif sub_sub_row["roleTitle"] == "Thumbnail":
                            loc_elem.attrib["role"] = "image_thumbnail"
                        elif sub_sub_row["roleTitle"] == "Databasbild":
                            loc_elem.attrib["role"] = "image_reference"
                        elif sub_sub_row["roleTitle"] == "sound_reference":
                            loc_elem.attrib["role"] = "sound_reference"

                        loc_elem.attrib["{%s}href" % xlink] = sub_sub_row["filePath"]

                if row["c_isReferencedBy_URL"]:
                    grp_elem = SubElement(did_elem, "{%s}daogrp" % namespace_map["ead"])
                    desc_elem = SubElement(grp_elem, "{%s}daodesc" % namespace_map["ead"])
                    p_elem = SubElement(desc_elem, "{%s}p" % namespace_map["ead"])
                    p_elem.text = row["c_isReferencedBy_URL"]
                    loc_elem = SubElement(grp_elem, "{%s}daoloc" % namespace_map["ead"],
                                          attrib={"{%s}label" % xlink: "context_www"}, nsmap=namespace_map)
                    loc_elem.text = ""
                    loc_elem.attrib["role"] = "url"
                    loc_elem.attrib["{%s}href" % xlink] = row["c_isReferencedBy_URL"]

                if row["dc_description"]:
                    scopecontent_elem = SubElement(c_elem, "{%s}scopecontent" % namespace_map["ead"])
                    head_elem = SubElement(scopecontent_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "description"
                    p_elem = SubElement(scopecontent_elem, "{%s}p" % namespace_map["ead"])
                    p_elem.text = row["dc_description"]

                    if row["dcterms_isReferencedBy"]:
                        p_elem = SubElement(scopecontent_elem, "{%s}p" % namespace_map["ead"])
                        p_elem.text = row["dcterms_isReferencedBy"]

                if row["dc_rights"]:
                    for elem_tag in ["{%s}userestrict" % namespace_map["ead"], "{%s}accessrestrict" % namespace_map["ead"]]:
                        restrict_elem = SubElement(c_elem, elem_tag)
                        if row["dc_rights"] == "CC BY 4.0":
                            p_elem = SubElement(restrict_elem, "{%s}p" % namespace_map["ead"])
                            p_elem.text = row["dc_rights"]
                            extptr_elem = SubElement(p_elem, "{%s}extptr" % namespace_map["ead"], attrib={"href": "https://creativecommons.org/licenses/by/4.0/"})
                            extptr_elem.text = ""
                        else:
                            p_elem = SubElement(restrict_elem, "{%s}p" % namespace_map["ead"],
                                                attrib={"lang": "swe"})
                            p_elem.text = row["dc_rights"]
                            p_elem = SubElement(restrict_elem, "{%s}p" % namespace_map["ead"],
                                                attrib={"lang": "fin"})
                            p_elem.text = row["rights_fin"]
                            p_elem = SubElement(restrict_elem, "{%s}p" % namespace_map["ead"],
                                                attrib={"lang": "eng"})
                            p_elem.text = row["rights_eng"]

                if row["dc_type"] or row["dc_type2"]:
                    control_access_elem = SubElement(c_elem, "{%s}controlaccess" % namespace_map["ead"])
                    head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "format"
                    dctype = row["dc_type"] if row["dc_type"] else ""

                    if ", " in dctype:
                        split_types = dctype.split(", ")
                        for split_type in split_types:
                            if split_type:
                                genreform_elem = SubElement(control_access_elem, "{%s}genreform" % namespace_map["ead"])
                                genreform_elem.text = split_type
                    elif dctype:
                        genreform_elem = SubElement(control_access_elem, "{%s}genreform" % namespace_map["ead"])
                        genreform_elem.text = dctype

                if row["dc_creator"]:
                    control_access_elem = SubElement(c_elem, "{%s}controlaccess" % namespace_map["ead"])
                    head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "author"
                    if "; " in row["dc_creator"]:
                        split_persons = row["dc_creator"].split("; ")
                        for person in split_persons:
                            if person:
                                persname_elem = SubElement(control_access_elem, "{%s}persname" % namespace_map["ead"],
                                                           attrib={"role": "creator"})
                                persname_elem.text = person

                    else:
                        persname_elem = SubElement(control_access_elem, "{%s}persname" % namespace_map["ead"],
                                                   attrib={"role": "creator"})
                        persname_elem.text = row["dc_creator"]

                if row["dc_subject"]:
                    persons = row["dc_subject"].split("; ")

                    control_access_elem = SubElement(c_elem, "{%s}controlaccess" % namespace_map["ead"])
                    head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "topic_facet"

                    for person in persons:
                        person_elements = person.split(" (")
                        if len(person_elements) > 1:
                            subject_elem = SubElement(control_access_elem, "{%s}subject" % namespace_map["ead"],
                                                      attrib={"href": person_elements[1].replace(")", "")})
                            subject_elem.text = person_elements[0]
                            subject_elem.attrib["source"] = "YSO"
                            subject_elem.attrib["lang"] = "swe"
                        else:
                            subject_elem = SubElement(control_access_elem, "{%s}subject" % namespace_map["ead"],
                                                      attrib={"rules": "internal"})
                            subject_elem.text = person_elements[0]

                if row["dcterms_spatial_full"]:
                    control_access_elem = SubElement(c_elem, "{%s}controlaccess" % namespace_map["ead"])
                    head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "geographic_facet"
                    if ", " in row["dcterms_spatial_full"]:
                        split_terms = row["dcterms_spatial_full"].split(", ")
                        for term in split_terms:
                            if term:
                                geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"],
                                                           attrib={"lang": "swe"})
                                geogname_elem.text = term
                    else:
                        geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"],
                                                   attrib={"lang": "swe"})
                        geogname_elem.text = row["dcterms_spatial_full"]

                if row["dcterms_spatial_fin"]:
                    control_access_elem = SubElement(c_elem, "{%s}controlaccess" % namespace_map["ead"])
                    head_elem = SubElement(control_access_elem, "{%s}head" % namespace_map["ead"])
                    head_elem.text = "geographic_facet"
                    if ", " in row["dcterms_spatial_fin"]:
                        split_terms = row["dcterms_spatial_fin"].split(", ")
                        for term in split_terms:
                            if term:
                                geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"],
                                                           attrib={"lang": "fin"})
                                geogname_elem.text = term
                    else:
                        geogname_elem = SubElement(control_access_elem, "{%s}geogname" % namespace_map["ead"],
                                                   attrib={"lang": "fin"})
                        geogname_elem.text = row["dcterms_spatial_fin"]


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

    return tostring(root_element, encoding="UTF-8", pretty_print=True, xml_declaration=True)


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
        sql_query = "SELECT MIN(GREATEST(digitalObjects.date_modify, intellectualEntities.date_modify, samlingar.date_modify)) AS date FROM digitalObjects, intellectualEntities, samlingar WHERE digitalObjects.c_ienummer = intellectualEntities.nummer AND intellectualEntities.c_samlingsnummer = samlingar.nummer"

    return sql_query
