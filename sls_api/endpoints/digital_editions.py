import calendar
from collections import OrderedDict
from flask import abort, Blueprint, safe_join, send_from_directory
from flask.json import jsonify
import io
import logging
from logging.handlers import TimedRotatingFileHandler
from lxml import etree
import os
from ruamel.yaml import YAML
from sqlalchemy import create_engine
import sqlalchemy.sql
import time
import re
import glob
from PIL import Image
from hashlib import md5
import base64

digital_edition = Blueprint('digital_edition', __name__)

config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs")
with io.open(os.path.join(config_dir, "digital_editions.yml"), encoding="UTF-8") as digital_editions_config:
    yaml = YAML()
    project_config = yaml.load(digital_editions_config)

logger = logging.getLogger("sls_api.digital_edition")

file_handler = TimedRotatingFileHandler(filename=project_config["log_file"], when="midnight", backupCount=7)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
logger.addHandler(file_handler)

db_engines = {}
for project, configuration in project_config.items():
    if isinstance(configuration, dict) and "engine" in configuration:
        db_engines[project] = create_engine(configuration["engine"])


class FileResolver(etree.Resolver):
    def resolve(self, system_url, public_id, context):
        logger.debug("Resolving {}".format(system_url))
        return self.resolve_filename(system_url, context)


def xml_to_html(xsl_file_path, xml_file_path, replace_namespace=True, params=None):
    logger.debug("Transforming {} using {}".format(xml_file_path, xsl_file_path))
    if params is not None:
        logger.debug("Parameters are {}".format(params))
    if not os.path.exists(xsl_file_path):
        return "XSL file {!r} not found!".format(xsl_file_path)
    if not os.path.exists(xml_file_path):
        return "XML file {!r} not found!".format(xml_file_path)

    with io.open(xml_file_path, mode="rb") as xml_file:
        xml_contents = xml_file.read()
        if replace_namespace:
            xml_contents = xml_contents.replace(b'xmlns="http://www.sls.fi/tei"', b'xmlns="http://www.tei-c.org/ns/1.0"')

        xml_root = etree.fromstring(xml_contents)

    xsl_parser = etree.XMLParser()
    xsl_parser.resolvers.add(FileResolver())
    with io.open(xsl_file_path, encoding="UTF-8") as xsl_file:
        xslt_root = etree.parse(xsl_file, parser=xsl_parser)
        xsl_transform = etree.XSLT(xslt_root)

    if params is None:
        result = xsl_transform(xml_root)
    elif isinstance(params, dict) or isinstance(params, OrderedDict):
        result = xsl_transform(xml_root, **params)
    else:
        raise Exception("Invalid parameters for XSLT transformation, must be of type dict or OrderedDict, not {}".format(type(params)))
    if len(xsl_transform.error_log) > 0:
        logging.debug(xsl_transform.error_log)
    return str(result)


def get_mysql_connection(database):
    if database not in db_engines:
        return None
    return db_engines[database].connect()


@digital_edition.after_request
def set_access_control_headers(response):
    if "Access-Control-Allow-Origin" not in response.headers:
        response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "X-Requested-With, Content-Type, Accept, Origin, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET"

    if response.headers.get("Content-Type") == "application/json":
        response.headers["Content-Type"] = "application/json;charset=utf-8"

    return response


# routes/digitaledition/html.php
@digital_edition.route("/<project>/html/<filename>")
def get_html_contents_as_json(project, filename):
    logger.info("Getting static content from /{}/html/{}".format(project, filename))
    file_path = safe_join(project_config[project]["file_root"], "html", "{}.html".format(filename))
    if os.path.exists(file_path):
        with io.open(file_path, encoding="UTF-8") as html_file:
            contents = html_file.read()
        data = {
            "filename": filename,
            "content": contents
        }
        return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
    else:
        abort(404)

# routes/digitaledition/md.php
@digital_edition.route("/<project>/md/<fileid>")
def get_md_contents_as_json(project, fileid):

    path = "*/".join(fileid.split("-")) + "*"

    file_path_query = safe_join(project_config[project]["file_root"], "md", path)

    try:
        file_path = [f for f in glob.iglob(file_path_query)][0]
        print(file_path)
        if os.path.exists(file_path):
            with io.open(file_path, encoding="UTF-8") as md_file:
                contents = md_file.read()
            data = {
                "fileid": fileid,
                "content": contents
            }
            return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
        else:
            abort(404)
    except:
        print(file_path_query)
        abort(404)

# routes/digitaledition/toc.php
@digital_edition.route("/<project>/static-pages-toc/<language>")
def get_static_pages_as_json(project, language):
    logger.info("Getting static content from /{}/static-pages-toc/{}".format(project, language))
    folder_path = safe_join(project_config[project]["file_root"], "md", language)
    logger.info("Checking for {}".format(folder_path))

    if os.path.exists(folder_path):
        data = path_hierarchy(folder_path, language)
        return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}
    else:
        logger.info("did not find {}".format(folder_path))
        abort(404)


# routes/digitaledition/manuscripts.php
@digital_edition.route("/<project>/manuscript/<publication_id>")
def get_manuscripts(project, publication_id):
    logger.info("Getting manuscript /{}/manuscript/{}".format(project, publication_id))
    connection = get_mysql_connection(project)
    sql = sqlalchemy.sql.text("SELECT * FROM manuscripts WHERE m_publication_id=:pub_id ORDER BY m_sort")
    statement = sql.bindparams(pub_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


# routes/digitaledition/publications.php
@digital_edition.route("/<project>/publication/<publication_id>")
def get_publication(project, publication_id):
    logger.info("Getting publication /{}/publication/{}".format(project, publication_id))
    connection = get_mysql_connection(project)
    sql = sqlalchemy.sql.text("SELECT * FROM publications WHERE p_id=:p_id ORDER BY p_title")
    statement = sql.bindparams(p_id=publication_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


def getFacsimileImage(project, edition_id, publication_id, size=(300,300)):
    logger.info("Getting facsimile image from funciton getFacsimileImage({},{},{})".format(project, edition_id, publication_id))

    outfile = md5("{}-{}-{}-{}".format(edition_id, publication_id, size[0], size[1]).encode('utf-8'))
    cache_file_path = safe_join(project_config[project]["file_root"], "cache", "faksimil", "{}.png".format(outfile))
    image_file_path = safe_join(project_config[project]["file_root"], "faksimil", edition_id, "{}.png".format(publication_id))
    if os.path.exists(cache_file_path):
        logger.debug("cache_file_path exists: {}".format(cache_file_path))
        return cache_file_path
    else:
        try:
            logger.debug("cache_file_path does not exist: {}".format(cache_file_path))
            logger.debug("Will create new image")
            im = Image.open(image_file_path)
            im.thumbnail(size, Image.ANTIALIAS)
            im.save(cache_file_path, "JPEG")
            logger.debug("I think it was successful")
            return cache_file_path
        except Exception as e:
            logger.debug("there was an exception: \n-----------\n{}\n-----------\n".format(str(e)))
            return ""

# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/facsimiles/<edition_id>/<publication_id>")
def get_facsimiles(project, edition_id, publication_id):
    logger.info("Getting facsimiles /{}/facsimiles/{}/{}".format(project, edition_id, publication_id))

    connection = get_mysql_connection(project)

    sql = """select f.*, fp.*, m_title, e.ed_id, fp.publications_id as fp_publications_id from publications_ed as e
    left join publications as p on p.p_ed_id=e.ed_id
    left join facsimile_publications as fp on p.p_id=fp.publications_id
    left outer join facsimiles as f on f.faksimil=fp.facs_id and f.publication_id=p.p_id
    left outer join manuscripts as m on m.m_id=fp.ms_id
    where p.p_id=:p_id and e.ed_id=:ed_id
    order by fp.priority"""


    if project_config.get(project).get("show_internally_published"):
        sql = " ".join([sql, "and e.ed_lansering>0"])
    elif not project_config.get(project).get("show_unpublished"):
        sql = " ".join([sql, "and e.ed_lansering>2"])

    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, p_id=publication_id)

    images = {}
    result = []
    for row in connection.execute(statement).fetchall():
        facsimile = dict(row)
        facsimile["start_url"] = safe_join(
                "digitaledition",
                project,
                "faksimil",
                str(row["facs_id"]),
                "1"
        )
        pre_pages = row["pre_page_count"] or 0

        facsimile["first_page"] = pre_pages + row["page_nr"]

        sql2 = "select * from facsimile_publications where facs_id=:facs_id and page_nr>:page_nr order by page_nr asc limit 1"
        statement2 = sqlalchemy.sql.text(sql2).bindparams(facs_id=row["facs_id"], page_nr=row["page_nr"])
        for row2 in connection.execute(statement2).fetchall():
            facsimile["last_page"] = pre_pages + row2["page_nr"] - 1

        if "last_page" not in facsimile.keys():
            facsimile["last_page"] = row["pages"]

        result.append(facsimile)
        '''try:
            with open(facsimile_image, "rb") as imageFile:
                facsimile["image_data"] = base64.b64encode(imageFile.read()).decode("utf-8")
        except:
            logger.error("Missing facsimile image: {}".format(facsimile_image))
        '''
    connection.close()

    return_data = []
    for row in result:
        if row["ed_id"] not in project_config.get(project).get("disabled_publications"):
            return_data.append(row)

    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/facsimile/<facs_id>/<size>/<page>")
def get_facsimile(project, facs_id, size, page):
    logger.info("Getting facsimile /{}/facsimile/{}/{}/{}".format(project, facs_id, size, page))

    connection = get_mysql_connection(project)
    sql = "select fp.*, f.title, f.pages, f.description, f.pdf, f.pre_page_count, e.ed_id, p.p_id from publications_ed as e left join publications as p on p.p_ed_id=e.ed_id left join facsimiles as f on f.publication_id=p.p_id left join facsimile_publications as fp on fp.publications_id=p.p_id where facs_id=:facs_id"

    if project_config.get(project).get("show_internally_published"):
        sql = " ".join([sql, "and e.ed_lansering>0"])
    elif not project_config.get(project).get("show_unpublished"):
        sql = " ".join([sql, "and e.ed_lansering>2"])

    statement = sqlalchemy.sql.text(sql).bindparams(facs_id=facs_id)
    if int(page) < 1:
        return jsonify("Image not found"), 404

    result = []
    for row in connection.execute(statement).fetchall():
        if int(page) > row["pages"]:
            return jsonify("Image not found"), 404

        folder = safe_join(project_config[project]["file_root"],
                           "faksimil", str(facs_id), str(size)
                           )
        return send_from_directory(folder, f"{page}.jpg")
        connection.close()
    return jsonify("Image not found"), 404

# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/editions")
def get_toc_editions(project):
    logger.info("Getting editions /{}/table-of-contqents/editions".format(project))
    connection = get_mysql_connection(project)
    if project_config.get(project).get("show_unpublished"):
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed ORDER BY ed_datumlansering"
    elif project_config.get(project).get("show_internally_published"):
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed WHERE ed_lansering>0 ORDER BY ed_datumlansering"
    else:
        sql = "SELECT ed_id AS id, ed_title AS title, ed_filediv AS divchapters FROM publications_ed WHERE ed_lansering=2 ORDER BY ed_datumlansering"

    statement = sqlalchemy.sql.text(sql)
    result = []
    for row in connection.execute(statement).fetchall():
        result.append(dict(row))
    connection.close()

    return_data = []
    for row in result:
        if row["id"] not in project_config.get(project).get("disabled_publications"):
            return_data.append(row)

    return jsonify(return_data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/root")
def get_toc_root(project, edition_id):
    logger.info("Getting root s/{}/table-of-contents/{}/root".format(project, edition_id))
    connection = get_mysql_connection(project)
    show_published = 2
    if project_config.get(project).get("show_unpublished"):
        show_published = 0
    elif project_config.get(project).get("show_internally_published"):
        show_published = 1

    if int(edition_id) == 15:
        sql = "SELECT toc.* FROM tableofcontents toc " \
              "JOIN publications_ed ped ON toc.toc_ed_id = ped.ed_id " \
              "JOIN publications_group pgroup ON toc.toc_group_id = pgroup.group_id " \
              "WHERE toc_ed_id=:ed_id AND pgroup.group_lansering>={} AND toc_groupid IS NULL ORDER BY sortOrder".format(show_published)
    else:
        sql = "SELECT toc.* FROM tableofcontents toc " \
              "WHERE toc_ed_id=:ed_id AND toc_group_id IS NULL AND toc_groupid IS NULL ORDER BY sortOrder"

    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()

    return jsonify(results)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/group/<group_id>")
def get_toc_root_elements(project, edition_id, group_id):
    logger.info("Getting \"root elements\" /{}/table-of-contents/edition/{}/group/{}".format(project, edition_id, group_id))
    connection = get_mysql_connection(project)
    sql = "SELECT * FROM tableofcontents WHERE toc_ed_id=:ed_id AND toc_groupid=:g_id AND toc_linkType!=6 ORDER BY sortOrder"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, g_id=group_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    if len(results) < 1:
        sql = "SELECT * FROM tableofcontents WHERE toc_ed_id=:ed_id AND toc_groupid=:g_id ORDER BY sortOrder"
        statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, g_id=group_id)
        for row in connection.execute(statement).fetchall():
            results.append(dict(row))
    connection.close()
    return jsonify(results)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/prevnext/<link_id>")
def get_toc_edition_links(project, edition_id, link_id):
    logger.info("Getting links /{}/table-of-contents/edition/{}/prevnext/{}".format(project, edition_id, link_id))
    return_data = OrderedDict()
    connection = get_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, " \
          "ed_filediv AS multiple_files, ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id=:ed_id ORDER BY ed_datumlansering"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id)
    edition_data = []
    for row in connection.execute(statement).fetchall():
        edition_data.append(dict(row))

    if edition_data[0]["multiple_files"] == 0:
        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id FROM tableofcontents t1 " \
              "LEFT JOIN tableofcontents t2 ON t1.toc_linkType=t2.toc_linkType AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=:ed_id AND t2.toc_linkID=:l_id AND t1.toc_id < t2.toc_id ORDER BY t1.sortorder DESC LIMIT 1"
        statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, l_id=link_id)
        toc_data = connection.execute(statement).fetchone()
        if toc_data:
            return_data["prev"] = dict(toc_data)
        else:
            return_data["prev"] = None

        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=:ed_id AND t2.toc_linkID=:l_id " \
              "AND t1.toc_id > t2.toc_id ORDER BY t1.sortorder ASC LIMIT 1"
        statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, l_id=link_id)
        toc_data = connection.execute(statement).fetchone()
        if toc_data:
            return_data["next"] = dict(toc_data)
        else:
            return_data["next"] = None
    else:
        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_groupid=t2.toc_groupid " \
              "AND t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=:ed_id AND t2.toc_linkID=:l_id " \
              "AND t1.toc_id < t2.toc_id ORDER BY t1.sortorder DESC LIMIT 1"
        statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, l_id=link_id)
        toc_data = connection.execute(statement).fetchone()
        if toc_data:
            return_data["prev"] = dict(toc_data)
        else:
            return_data["prev"] = None

        sql = "SELECT t1.title AS title, t1.toc_ed_id AS edition_id, t1.toc_linkID AS link_id " \
              "FROM tableofcontents t1 LEFT JOIN tableofcontents t2 " \
              "ON t1.toc_groupid=t2.toc_groupid " \
              "AND t1.toc_linkType=t2.toc_linkType " \
              "AND t1.toc_ed_id=t2.toc_ed_id " \
              "WHERE t2.toc_ed_id=:ed_id AND t2.toc_linkID=:l_id " \
              "AND t1.toc_id > t2.toc_id ORDER BY t1.sortorder ASC LIMIT 1"
        statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id, l_id=link_id)
        toc_data = connection.execute(statement).fetchone()
        if toc_data:
            return_data["next"] = dict(toc_data)
        else:
            return_data["next"] = None

    connection.close()
    return jsonify(return_data)


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>")
def get_toc_edition(project, edition_id):
    logger.info("Getting edition /{}/table-of-contents/edition/{}".format(project, edition_id))
    connection = get_mysql_connection(project)

    show_published = 2
    if project_config.get(project).get("show_unpublished"):
        show_published = 0
    elif project_config.get(project).get("show_internally_published"):
        show_published = 1

    if edition_id == 15:
        sql = "SELECT toc.* FROM tableofcontents toc JOIN publications_ed ped ON toc.toc_ed_id = ped.ed_id " \
              "JOIN publications_group pgroup ON toc.toc_group_id = pgroup.group_id " \
              "WHERE toc_ed_id = :ed_id AND (toc_linkType!=6 OR toc_linkType IS NULL) AND prgroup.group_lansering>={} " \
              "ORDER BY toc_groupid, toc.sortOrder".format(show_published)
    else:
        sql = "SELECT toc.* FROM tableofcontents toc WHERE toc_ed_id=:ed_id AND (toc_linkType!=6 OR toc_linkType IS NULL) " \
              "AND toc.toc_group_id IS NULL ORDER BY toc_groupid, toc.sortOrder"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id)
    toc_data = []
    for row in connection.execute(statement).fetchall():
        toc_data.append(dict(row))
    connection.close()

    return_data = OrderedDict()
    sub_group_toc_id = 0
    for iteration in range(1, 3):
        for row in toc_data:
            if iteration == 1:
                if row["toc_groupid"] is None:
                    if row["toc_id"] not in return_data:
                        new_data = {
                            "id": row["toc_id"],
                            "title": row["title"],
                            "titleLevel": row["titleLevel"],
                            "items": []
                        }
                        return_data[int(row["toc_id"])] = new_data
            elif iteration == 2:
                if row["toc_groupid"] is not None and row["toc_linkType"] is None:
                    groupid = row["toc_groupid"]
                    if groupid not in return_data or "items" not in return_data[groupid] or row["toc_id"] not in \
                            return_data[groupid]["items"]:
                        # The PHP version builds up these objects, but appears to throw them away or lose them at some point?
                        continue
                        # sub_group_toc_id = row["toc_id"]
                        # new_data = {
                        #     "title": row["title"],
                        #     "id": row["toc_id"],
                        #     "items": []
                        # }
                        # if "items" not in return_data[groupid]:
                        #     return_data[groupid]["items"] = []
                        # return_data[groupid]["items"].insert(row["toc_id"], new_data)
                else:
                    if row["toc_groupid"] is None:
                        continue
                    new_data = {
                        "title": row["title"],
                        "id": row["toc_id"],
                        "link": "{}_{}".format(row["toc_ed_id"], row["toc_linkID"]),
                        "link_type": row["toc_linkType"],
                        "sort_order": row["sortOrder"]
                    }
                    try:
                        return_data[row["toc_groupid"]]["items"][sub_group_toc_id]["items"].insert(row["toc_id"], new_data)
                    except IndexError:
                        return_data[row["toc_groupid"]]["items"].insert(sub_group_toc_id, {"items": [new_data]})

    for key, value in return_data.items():
        # the PHP version seems to drop these at some point
        del value["id"]
        del value["title"]
        del value["titleLevel"]

    return jsonify(list(return_data.values()))


# routes/digitaledition/table-of-contents.php
@digital_edition.route("/<project>/table-of-contents/edition/<edition_id>/first")
def get_toc_edition_firstentry(project, edition_id):
    logger.info("Getting first edition /{}/table-of-contents/edition/{}/first".format(project, edition_id))
    connection = get_mysql_connection(project)
    sql = "SELECT title, toc_ed_id, toc_linkID FROM tableofcontents WHERE toc_ed_id=:ed_id AND toc_linkID IS NOT NULL ORDER BY sortOrder ASC LIMIT 1"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id)
    result = connection.execute(statement).fetchone()
    connection.close()
    if result is not None:
        result = dict(result)
    return jsonify(result)


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/est/<edition_id>")
def get_publication_est_text(project, edition_id):
    can_show, content = publish_status(project, edition_id)

    if can_show:
        id_parts = edition_id.replace("_est", "").split(";")
        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))
        logger.info("Getting contents for file {}".format(xml_file_path))

        xsl_file_path = safe_join(project_config["xslt_root"], "est.xsl")
        cache_file_path = safe_join(project_config[project]["file_root"], "cache", "est", "{}_est.html".format(id_parts[0]))

        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))

        content = None
        if os.path.exists(cache_file_path):
            if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
                try:
                    with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                        content = cache_file.read()
                except Exception:
                    content = "Error reading file from cache."
                else:
                    logger.info("Content fetched from cache.")
            else:
                logger.info("Cache file is old or invalid, deleting cache file...")
                os.remove(cache_file_path)
        if os.path.exists(xml_file_path) and content is None:
            logger.info("Getting contents from file and transforming...")
            try:
                content = xml_to_html(xsl_file_path, xml_file_path)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
                        cache_file.write(content)
                except Exception:
                    logger.exception("Could not create cachefile")
                    content = "Successfully fetched content but could not generate cache for it."
            except Exception:
                logger.exception("Error when parsing XML file")
                content = "Error parsing document."
        elif content is None:
            content = "File not found."

    data = {
        "id": edition_id,
        "content": content.replace("id=", "data-id=")
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/com/<edition_id>/<note_id>")
@digital_edition.route("/<project>/text/com/<edition_id>")
def get_publication_com_text(project, edition_id, note_id=None):
    can_show, content = publish_status(project, edition_id)

    if can_show:
        id_parts = edition_id.replace("_com", "").split(";")

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", "com", "{}_com.xml".format(id_parts[0]))
        est_file_path = safe_join(project_config[project]["file_root"], "xml", "est", "{}_est.xml".format(id_parts[0]))
        logger.info("Getting contents for file {}".format(xml_file_path))

        params = {
            "estDocument": '"file://{}"'.format(est_file_path)
        }
        xsl_file = "com.xsl"
        if note_id is not None:
            params["noteId"] = '"{}"'.format(note_id)
            xsl_file = "notes.xsl"
        xsl_file_path = safe_join(project_config["xslt_root"], xsl_file)
        cache_file_path = safe_join(project_config[project]["file_root"], "cache", "com", "note_{}_com_{}.html".format(id_parts[0], note_id))
        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))
        logger.debug("est XML file path is {}".format(est_file_path))

        content = None
        if os.path.exists(cache_file_path):
            if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):  # TODO also check est file mtime
                try:
                    with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                        content = cache_file.read()
                except Exception:
                    content = "Error reading content from cache."
                else:
                    logger.info("Content fetched from cache.")
            else:
                logger.info("Cache file is old or invalid, deleting cache file...")
                os.remove(cache_file_path)
        if os.path.exists(xml_file_path) and content is None:
            logger.info("Getting contents from file and transforming...")
            try:
                content = xml_to_html(xsl_file_path, xml_file_path, params=params)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
                        cache_file.write(content)
                except Exception:
                    logger.exception("Could not create cachefile")
                    content = "Successfully fetched content but could not generate cache for it."
            except Exception:
                logger.exception("Error when parsing XML file")
                content = "Error parsing document"
        elif content is None:
            content = "File not found"

    data = {
        "id": edition_id,
        "content": content
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/ms/<edition_id>")
@digital_edition.route("/<project>/text/ms/<edition_id>/<changes>")
def get_publication_manuscripts(project, edition_id, changes=False):
    can_show, error_message = publish_status(project, edition_id)

    if can_show:
        item_id, book_id, text_id, section_id = get_id_parts(edition_id)
        connection = get_mysql_connection(project)

        # the content has chapters in the same xml
        sql = "SELECT m_title as title, m_type as type, m_filename as filename, m_id as id FROM manuscripts WHERE m_filename LIKE :f_name ORDER BY m_sort"
        statement = sqlalchemy.sql.text(sql).bindparams(f_name=item_id + "_ms_%")
        manuscript_info = []
        for row in connection.execute(statement).fetchall():
            manuscript_info.append(dict(row))
        connection.close()

        for i in range(len(manuscript_info)):
            manuscript = manuscript_info[i]
            params = {
                "bookId": book_id
            }
            manuscript_info[i]["manuscript_changes"] = get_content(project, "ms", manuscript["filename"], "ms_changes.xsl", params)
            manuscript_info[i]["manuscript_normalized"] = get_content(project, "ms", manuscript["filename"], "ms_normalized.xsl", params)

        data = {
            "id": item_id,
            "manuscripts": manuscript_info
        }

    else:
        data = {
            "id": edition_id,
            "error": error_message
        }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/var/<edition_id>")
def get_publication_variations(project, edition_id):
    logger.info("Getting XML /{}/text/var/{} and transforming ...".format(project, edition_id))
    can_show, error_message = publish_status(project, edition_id)

    if can_show:
        item_id, book_id, text_id, section_id = get_id_parts(edition_id)
        connection = get_mysql_connection(project)

        # the content has chapters in the same xml
        if section_id is not None:
            sql = "SELECT v_title as title, v_type as type, v_filename as filename, v_id as id FROM versions WHERE v_filename LIKE :f_name AND v_section_id=:s_id ORDER BY v_sort"
            statement = sqlalchemy.sql.text(sql).bindparams(f_name=item_id + "_var_%", s_id=section_id)
            variation_info = []
            for row in connection.execute(statement).fetchall():
                variation_info.append(dict(row))
        else:
            sql = "SELECT v_title as title, v_type as type, v_filename as filename, v_id as id FROM versions WHERE v_filename LIKE :f_name ORDER BY v_sort"
            statement = sqlalchemy.sql.text(sql).bindparams(f_name=item_id + "_var_%")
            variation_info = []
            for row in connection.execute(statement).fetchall():
                variation_info.append(dict(row))
        connection.close()

        for i in range(len(variation_info)):
            variation = variation_info[i]
            params = {
                "bookId": book_id
            }
            # chapters_xsl_file = "chapters.xsl"

            if variation["type"] == "1":
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if section_id is not None:
                params["sectionId"] = section_id

            variation_info[i]["content"] = get_content(project, "var", variation["filename"], xsl_file, params)

        data = {
            "id": edition_id,
            "variations": variation_info
        }

    else:
        data = {
            "id": edition_id,
            "error": error_message
        }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/inl/<edition_id>")
@digital_edition.route("/<project>/text/inl/<edition_id>/<lang>")
def get_publication_inl_text(project, edition_id, lang=None):
    return get_publication_inl_tit_text(project, edition_id, lang, "inl")


# routes/digitaledition/xml.php
@digital_edition.route("/<project>/text/tit/<edition_id>")
@digital_edition.route("/<project>/text/tit/<edition_id>/<lang>")
def get_publication_tit_text(project, edition_id, lang=None):
    return get_publication_inl_tit_text(project, edition_id, lang, "tit")


def get_publication_inl_tit_text(project, edition_id, lang=None, what="inl"):
    can_show, content = publish_status(project, edition_id)

    if can_show:
        lang_code = "fin" if lang == "fi" else "swe"
        version = "int" if project_config[project]["show_internally_published"] else "ext"
        filename = "{}_{}_{}_{}.xml".format(edition_id, what, lang_code, version)

        xml_file_path = safe_join(project_config[project]["file_root"], "xml", what, filename)

        logger.info("Getting contents for file {}".format(xml_file_path))

        if what == "tit":
            xsl_file = "title.xsl"
        else:
            xsl_file = "est.xsl"
        xsl_file_path = safe_join(project_config["xslt_root"], xsl_file)
        cache_file_path = safe_join(project_config[project]["file_root"], "cache", what, filename.replace(".xml", ".html"))

        logger.debug("Cache file path is {}".format(cache_file_path))
        logger.debug("XML file path is {}".format(xml_file_path))

        content = None
        if os.path.exists(cache_file_path):
            if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
                try:
                    with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                        content = cache_file.read()
                except Exception:
                    logger.exception("Error reading content from cache")
                    content = "Error reading content from cache"
                else:
                    logger.info("Content fetched from cache.")
            else:
                logger.info("Cache file is old or invalid, deleting cache file...")
                os.remove(cache_file_path)
        if os.path.exists(xml_file_path) and content is None:
            logger.info("Getting contents from file and transforming...")
            try:
                content = xml_to_html(xsl_file_path, xml_file_path)
                try:
                    with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                        logger.info("Writing contents to cache file")
                        cache_file.write(content)
                except Exception:
                    logger.exception("Could not create cachefile")
                    content = "Successfully fetched content but could not generate cache for it."
            except Exception:
                logger.exception("Error parsing document")
                content = "Error parsing document"
        elif content is None:
            logger.warning("No preface found")
            content = "File not found"

    data = {
        "id": edition_id,
        "content": content
    }

    return jsonify(data), 200, {"Access-Control-Allow-Origin": "*"}


# routes/semantic_data/persons.php
@digital_edition.route("/semantic_data/persons/tooltip/<person_id>")
def get_person_tooltip(person_id):
    logger.info("Getting tooltip /semantic_data/persons/tooltip/{}".format(person_id))
    connection = get_mysql_connection("semantic_data")
    sql = "SELECT c_webbnamn_1_sort AS title, ed_tooltip AS content, c_webbfornamn1, c_webbefternamn1 " \
          "FROM persons WHERE id_p=:p_id"
    statement = sqlalchemy.sql.text(sql).bindparams(p_id=person_id)
    ms_data = connection.execute(statement).fetchone()
    connection.close()
    if ms_data:
        return jsonify(dict(ms_data))
    else:
        return jsonify("Person not found"), 404


# routes/semantic_data/persons.php
@digital_edition.route("/semantic_data/persons/list/<data_source_id>")
def get_list_of_persons(data_source_id):
    logger.info("Getting list of persons /semantic_data/persons/list/{}".format(data_source_id))
    connection = get_mysql_connection("semantic_data")
    sql = "SELECT c_webbnamn_1_sort AS title, ed_tooltip AS content, " \
          "c_webbfornamn1, c_webbefternamn1, ed_tooltip, id_p, c_webbsok " \
          "FROM persons WHERE data_source_id=:ds_id ORDER BY id_p ASC"
    statement = sqlalchemy.sql.text(sql).bindparams(ds_id=data_source_id)
    ms_data = []
    for row in connection.execute(statement).fetchall():
        ms_data.append(dict(row))
    connection.close()
    return jsonify(ms_data)


# routes/semantic_data/places.php
@digital_edition.route("/semantic_data/places/tooltip/<place_id>")
def get_place_tooltip(place_id):
    logger.info("Getting tooltip /semantic_data/places/tooltip/{}".format(place_id))
    connection = get_mysql_connection("semantic_data")
    place_id = place_id.replace("pl", "").replace("PlId", "")

    sql = "SELECT o_ortnamn AS title, o_beskrivning AS content FROM places WHERE id=:p_id"
    statement = sqlalchemy.sql.text(sql).bindparams(p_id=place_id)
    ms_data = connection.execute(statement).fetchone()
    connection.close()
    if ms_data:
        return jsonify(dict(ms_data))
    else:
        return jsonify("Place not found."), 404


# routes/semantic_data/places.php
@digital_edition.route("/semantic_data/places/list")
def get_list_of_places():
    logger.info("Getting list of places /semantic_data/places/list")
    connection = get_mysql_connection("semantic_data")

    sql = "SELECT c_webbsok, o_id AS id FROM places ORDER BY o_id ASC"
    statement = sqlalchemy.sql.text(sql)
    ms_data = []
    for row in connection.execute(statement).fetchall():
        ms_data.append(dict(row))
    connection.close()
    return jsonify(ms_data)


'''
    HELPER FUNCTIONS  
'''


def slugify_route(path):
    path = path.replace(" - ", "")
    path = path.replace(" ", "-")
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('[^a-zA-Z0-9\\\/-]|_', '', re.sub('.md', '', path))
    return path.lower()


def slugify_id(path, language):
    path = re.sub('[^0-9]', '', path) 
    path = language + path
    path = '-'.join(path[i:i+2] for i in range(0, len(path), 2))
    return path


def slugify_path(path):
    path = split_after(path, "/topelius_required/md/")
    return re.sub('.md', '', path)


def path_hierarchy(path, language):
    hierarchy = {'id': slugify_id(path, language), 'title': filter_title(os.path.basename(path)),
                 'basename': re.sub('.md', '', os.path.basename(path)), 'path': slugify_path(path), 'fullpath': path,
                 'route': slugify_route(split_after(path, "/topelius_required/md/")), 'type': 'folder',
                 'children': [path_hierarchy(p, language) for p in glob.glob(os.path.join(path, '*'))]}

    if not hierarchy['children']:
        del hierarchy['children']
        hierarchy['type'] = 'file'

    return dict(hierarchy)


def filter_title(path):
    path = ''.join([i for i in path.lstrip('-') if not i.isdigit()])
    path = re.sub('-', '', path)
    path = re.sub('.md', '', path)
    return path.strip()


def split_after(value, a):
    pos_a = value.rfind(a)
    if pos_a == -1: return ""
    adjusted_pos_a = pos_a + len(a)
    if adjusted_pos_a >= len(value): return ""
    return value[adjusted_pos_a:]


def cache_is_recent(source_file, xsl_file, cache_file):
    """
    Returns False if the source or xsl file have been modified since the creation of the cache file
    Returns False if the cache is more than 'cache_lifetime_seconds' seconds old, as defined in config file
    Otherwise, returns True
    """
    try:
        source_file_mtime = os.path.getmtime(source_file)
        xsl_file_mtime = os.path.getmtime(xsl_file)
        cache_file_mtime = os.path.getmtime(cache_file)
    except OSError:
        return False
    if source_file_mtime > cache_file_mtime or xsl_file_mtime > cache_file_mtime:
        return False
    elif calendar.timegm(time.gmtime()) > (cache_file_mtime + project_config["cache_lifetime_seconds"]):
        return False
    return True


def publish_status(project, edition_id):
    """Get info on the publications status
    Is is visible to the public etc...

    returns two values:
        - a booelan if the publication can be shown
        - a message text why it can't be shown, if that is the case.
    """
    logger.info("Checking if /{} {} is published".format(project, edition_id))

    connection = get_mysql_connection(project)
    sql = "SELECT ed_id AS id, ed_lansering, ed_title AS title, ed_filediv AS multiple_files, " \
          "ed_date_swe AS info_sv, ed_date_fin AS info_fi " \
          "FROM publications_ed WHERE ed_id = :ed_id ORDER BY ed_datumlansering"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id.split("_")[0])  # edition_id is like 1_1, we need the first digit here
    edition_data = connection.execute(statement).fetchone()
    if edition_data:
        edition_data = dict(edition_data)

    sql = "SELECT DISTINCT p.p_identifier FROM digital_edition_topelius.publications_ed ped " \
          "JOIN digital_edition_topelius.publications p ON p.p_ed_id = ped.ed_id " \
          "JOIN digital_edition_topelius.publications_collection pc ON pc.coll_ed_id = p.p_ed_id " \
          "JOIN digital_edition_topelius.publications_group pg ON pg.group_id = p.p_group_id " \
          "WHERE ped.ed_lansering = 2 AND pg.group_lansering != 1 AND ped.ed_id = 15 AND p.p_identifier = :ed_id"
    statement = sqlalchemy.sql.text(sql).bindparams(ed_id=edition_id)
    letters = []
    for row in connection.execute(statement).fetchall():
        letters.append(dict(row))
    letter_published = False
    connection.close()

    if len(letters) > 0 or not edition_id.startswith("15_"):
        letter_published = True

    can_show = False
    content = ""

    if not edition_data:
        content = "Content does not exist"
    elif edition_data["ed_lansering"] < 1 and not project_config[project]["show_unpublished"]:
        content = "Content is not published"
    elif edition_data["ed_lansering"] == 1 and not project_config[project]["show_internally_published"] and not project_config[project]["show_unpublished"]:
        content = "Content is not externally published"
    else:
        if not project_config[project]["show_internally_published"] and not letter_published:
            content = "Content is not externally published"
        else:
            can_show = True

    return can_show, content


def get_id_parts(edition_id):
    id_parts = edition_id.replace("_est", "").split(";")  # 12_1_est;ch5  - Finland framställt i teckningar.

    item_id = id_parts[0]  # 1_1
    item_parts = item_id.split("_")
    book_id = item_parts[0]
    text_id = item_parts[1]
    section_id = id_parts[1] if len(id_parts) > 1 else None

    return item_id, book_id, text_id, section_id


def get_content(project, folder, xml_filename, xsl_filename, parameters):
    xml_file_path = safe_join(project_config[project]["file_root"], "xml", folder, xml_filename)
    xsl_file_path = safe_join(project_config["xslt_root"], xsl_filename)
    cache_file_path = xml_file_path.replace("/xml/", "/cache/").replace(".xml", ".html")
    content = None

    if os.path.exists(cache_file_path):
        if cache_is_recent(xml_file_path, xsl_file_path, cache_file_path):
            try:
                with io.open(cache_file_path, encoding="UTF-8") as cache_file:
                    content = cache_file.read()
            except Exception:
                content = "Error reading content from cache."
            else:
                logger.info("Content fetched from cache.")
        else:
            logger.info("Cache file is old or invalid, deleting cache file...")
            os.remove(cache_file_path)
    if os.path.exists(xml_file_path) and content is None:
        logger.info("Getting contents from file and transforming...")
        try:
            content = xml_to_html(xsl_file_path, xml_file_path, params=parameters).replace('\n', '').replace('\r', '')
            try:
                with io.open(cache_file_path, mode="w", encoding="UTF-8") as cache_file:
                    cache_file.write(content)
            except Exception:
                logger.exception("Could not create cachefile")
                content = "Successfully fetched content but could not generate cache for it."
        except Exception as e:
            logger.exception("Error when parsing XML file")
            content = "Error parsing document"
            content += str(e)
    elif content is None:
        content = "File not found"

    return content
