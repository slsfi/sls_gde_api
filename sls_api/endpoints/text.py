from flask import Blueprint, jsonify, request
import logging
import sqlalchemy
from werkzeug.security import safe_join

from sls_api.endpoints.generics import db_engine, get_collection_published_status, get_content, get_xml_content, get_project_config, get_published_status, get_collection_legacy_id

text = Blueprint('text', __name__)
logger = logging.getLogger("sls_api.text")

# Text functions


@text.route("/<project>/text/<text_type>/<text_id>")
def get_text_by_type(project, text_type, text_id):
    logger.info("Getting text by type /{}/text/{}/{}".format(project, text_type, text_id))

    text_table = ''
    if text_type == 'manuscript':
        text_table = 'publication_manuscript'
    elif text_type == 'variation':
        text_table = 'publication_version'
    elif text_type == 'commentary':
        text_table = 'publication_comment'
    elif text_type == 'facsimile':
        text_table = 'publication_facsimile'

    connection = db_engine.connect()
    sql = sqlalchemy.sql.text("SELECT * FROM {} WHERE id=:t_id".format(text_table))
    statement = sql.bindparams(t_id=text_id)
    results = []
    for row in connection.execute(statement).fetchall():
        results.append(dict(row))
    connection.close()
    return jsonify(results)


@text.route("/<project>/text/<collection_id>/<publication_id>/inl")
@text.route("/<project>/text/<collection_id>/<publication_id>/inl/<lang>")
def get_introduction(project, collection_id, publication_id, lang="swe"):
    """
    Get introduction text for a given publication @TODO: remove publication_id, it is not needed.
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_collection_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_introduction table? how handle language/version
            filename = "{}_inl_{}_{}.xml".format(collection_id, lang, version)
            xsl_file = "introduction.xsl"
            content = get_content(project, "inl", filename, xsl_file, None)
            data = {
                "id": "{}_{}_inl".format(collection_id, publication_id),
                "content": content.replace(" id=", " data-id=")
            }
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/tit")
@text.route("/<project>/text/<collection_id>/<publication_id>/tit/<lang>")
def get_title(project, collection_id, publication_id, lang="swe"):
    """
    Get title page for a given publication @TODO: remove publication_id, it is not needed?
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_collection_published_status(project, collection_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            version = "int" if config["show_internally_published"] else "ext"
            # TODO get original_filename from publication_collection_title table? how handle language/version
            filename = "{}_tit_{}_{}.xml".format(collection_id, lang, version)
            xsl_file = "title.xsl"
            content = get_content(project, "tit", filename, xsl_file, None)
            data = {
                "id": "{}_{}_tit".format(collection_id, publication_id),
                "content": content.replace(" id=", " data-id=")
            }
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/est")
def get_reading_text(project, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT legacy_id FROM publication WHERE id = :p_id AND original_filename IS NULL"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()
        if result is None or language is not None:
            filename = "{}_{}_est.xml".format(collection_id, publication_id)
            if language is not None:
                filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)
                logger.debug("Filename (est) for {} is {}".format(publication_id, filename))

            connection.close()
        else:
            filename = "{}_est.xml".format(result["legacy_id"])
            connection.close()
        logger.debug("Filename (est) for {} is {}".format(publication_id, filename))
        xsl_file = "est.xsl"

        bookId = get_collection_legacy_id(collection_id)
        if bookId is None:
            bookId = collection_id
        bookId = '"{}"'.format(bookId)

        if section_id is not None:
            section_id = '"{}"'.format(section_id)
            content = get_content(project, "est", filename, xsl_file,
                                  {"bookId": bookId, "sectionId": section_id})
        else:
            content = get_content(project, "est", filename, xsl_file, {"bookId": bookId})
        data = {
            # @TODO: investigate if id should have language in its value or not (similar to filename).
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content.replace(" id=", " data-id=")
        }
        if language is not None:
            data["language"] = language
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/com")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/com/<note_id>/<section_id>")
def get_comments(project, collection_id, publication_id, note_id=None, section_id=None):
    """
    Get comments file text for a given publication
    """
    config = get_project_config(project)
    if config is None:
        return jsonify({"msg": "No such project."}), 400
    else:
        can_show, message = get_published_status(project, collection_id, publication_id)
        if can_show:
            logger.info("Getting XML for {} and transforming...".format(request.full_path))
            connection = db_engine.connect()
            select = "SELECT legacy_id FROM publication_comment WHERE id IN (SELECT publication_comment_id FROM publication WHERE id = :p_id) \
                        AND legacy_id IS NOT NULL AND original_filename IS NULL"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            result = connection.execute(statement).fetchone()

            bookId = get_collection_legacy_id(collection_id)
            if bookId is None:
                bookId = collection_id

            bookId = '"{}"'.format(bookId)

            if result is not None:
                filename = "{}_com.xml".format(result["legacy_id"])
                connection.close()
            else:
                filename = "{}_{}_com.xml".format(collection_id, publication_id)
                connection.close()
            logger.debug("Filename (com) for {} is {}".format(publication_id, filename))
            params = {
                "estDocument": '"file://{}"'.format(safe_join(config["file_root"], "xml", "est", filename.replace("com", "est"))),
                "bookId": bookId
            }

            if note_id is not None and section_id is None:
                params["noteId"] = '"{}"'.format(note_id)
                xsl_file = "notes.xsl"
            else:
                xsl_file = "com.xsl"

            if section_id is not None:
                section_id = '"{}"'.format(section_id)
                content = get_content(project, "com", filename, xsl_file, {
                    "sectionId": str(section_id),
                    "estDocument": '"file://{}"'.format(safe_join(config["file_root"], "xml", "est", filename.replace("com", "est"))),
                    "bookId": bookId
                })
            else:
                content = get_content(project, "com", filename, xsl_file, params)

            data = {
                "id": "{}_{}_com".format(collection_id, publication_id),
                "content": content
            }
            connection.close()
            return jsonify(data), 200
        else:
            return jsonify({
                "id": "{}_{}".format(collection_id, publication_id),
                "error": message
            }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms")
@text.route("/<project>/text/<collection_id>/<publication_id>/list/ms/<section_id>")
def get_manuscript_list(project, collection_id, publication_id, section_id=None):
    """
    Get all manuscripts for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        connection = db_engine.connect()
        if section_id is not None:
            section_id = str(section_id).replace('ch', '')
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id AND section_id = :section AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id, section=section_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()
        else:
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()

        data = {
            "id": "{}_{}".format(collection_id, publication_id),
            "manuscripts": manuscript_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}_ms".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/ms/")
@text.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>")
@text.route("/<project>/text/<collection_id>/<publication_id>/ms/<manuscript_id>/<section_id>")
def get_manuscript(project, collection_id, publication_id, manuscript_id=None, section_id=None):
    """
    Get one or all manuscripts for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        if manuscript_id is not None and 'ch' not in str(manuscript_id):
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE id = :m_id AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(m_id=manuscript_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()
        else:
            select = "SELECT sort_order, name, legacy_id, id, original_filename FROM publication_manuscript WHERE publication_id = :p_id AND deleted != 1 ORDER BY sort_order ASC"
            statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
            manuscript_info = []
            for row in connection.execute(statement).fetchall():
                manuscript_info.append(dict(row))
            connection.close()

        bookId = get_collection_legacy_id(collection_id)
        if bookId is None:
            bookId = collection_id

        bookId = '"{}"'.format(bookId)

        for index in range(len(manuscript_info)):
            manuscript = manuscript_info[index]
            if section_id is not None:
                section_id = '"{}"'.format(section_id)
                params = {
                    "bookId": bookId,
                    "sectionId": str(section_id)
                }
            elif manuscript_id is not None and 'ch' in str(manuscript_id):
                section_id = '"{}"'.format(manuscript_id)
                params = {
                    "bookId": bookId,
                    "sectionId": str(section_id)
                }
            else:
                params = {
                    "bookId": bookId
                }
            if manuscript["original_filename"] is None and manuscript["legacy_id"] is not None:
                filename = "{}.xml".format(manuscript["legacy_id"])
            else:
                filename = "{}_{}_ms_{}.xml".format(collection_id, publication_id, manuscript["id"])
            manuscript_info[index]["manuscript_changes"] = get_content(project, "ms", filename, "ms_changes.xsl", params).replace(" id=", " data-id=")
            manuscript_info[index]["manuscript_normalized"] = get_content(project, "ms", filename, "ms_normalized.xsl", params).replace(" id=", " data-id=")

        data = {
            "id": "{}_{}".format(collection_id, publication_id),
            "manuscripts": manuscript_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}_ms".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<collection_id>/<publication_id>/var/")
@text.route("/<project>/text/<collection_id>/<publication_id>/var/<section_id>")
def get_variant(project, collection_id, publication_id, section_id=None):
    """
    Get all variants for a given publication, optionally specifying a section (chapter)
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} and transforming...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT sort_order, name, type, legacy_id, id, original_filename FROM publication_version WHERE publication_id = :p_id AND deleted != 1 ORDER BY type, sort_order ASC"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        variation_info = []
        for row in connection.execute(statement).fetchall():
            variation_info.append(dict(row))
        connection.close()

        bookId = get_collection_legacy_id(collection_id)
        if bookId is None:
            bookId = collection_id

        bookId = '"{}"'.format(bookId)
        if section_id is not None:
            section_id = '"{}"'.format(section_id)
            params = {
                "bookId": bookId,
                "sectionId": str(section_id)
            }
        else:
            params = {
                "bookId": bookId
            }

        for index in range(len(variation_info)):
            variation = variation_info[index]

            if variation["type"] == 1:
                xsl_file = "poem_variants_est.xsl"
            else:
                xsl_file = "poem_variants_other.xsl"

            if variation["original_filename"] is None and variation["legacy_id"] is not None:
                filename = "{}.xml".format(variation["legacy_id"])
            else:
                filename = "{}_{}_var_{}.xml".format(collection_id, publication_id, variation["id"])

            variation_info[index]["content"] = get_content(project, "var", filename, xsl_file, params)

        data = {
            "id": "{}_{}_var".format(collection_id, publication_id),
            "variations": variation_info
        }
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403


@text.route("/<project>/text/<format>/<collection_id>/<publication_id>/est-i18n/<language>")
@text.route("/<project>/text/<format>/<collection_id>/<publication_id>/est/<section_id>")
@text.route("/<project>/text/<format>/<collection_id>/<publication_id>/est")
def get_reading_text_downloadable_format(project, format, collection_id, publication_id, section_id=None, language=None):
    """
    Get reading text in a downloadable format for a given publication
    """
    can_show, message = get_published_status(project, collection_id, publication_id)
    if can_show:
        logger.info("Getting XML for {} ...".format(request.full_path))
        connection = db_engine.connect()
        select = "SELECT legacy_id FROM publication WHERE id = :p_id AND original_filename IS NULL"
        statement = sqlalchemy.sql.text(select).bindparams(p_id=publication_id)
        result = connection.execute(statement).fetchone()
        if result is None or language is not None:
            filename = "{}_{}_est.xml".format(collection_id, publication_id)
            if language is not None:
                filename = "{}_{}_{}_est.xml".format(collection_id, publication_id, language)
                logger.debug("Filename (est xml) for {} is {}".format(publication_id, filename))

            connection.close()
        else:
            filename = "{}_est.xml".format(result["legacy_id"])
            connection.close()
        logger.debug("Filename (est xml) for {} is {}".format(publication_id, filename))

        if format == "xml":
            xsl_file = "est_downloadable_xml.xsl"
        elif format == "plaintext":
            xsl_file = "est_downloadable_plaintext.xsl"
        else:
            xsl_file = None

        bookId = get_collection_legacy_id(collection_id)
        if bookId is None:
            bookId = collection_id
        bookId = '"{}"'.format(bookId)

        if section_id is not None:
            section_id = '"{}"'.format(section_id)
            content = get_xml_content(project, "est", filename, xsl_file,
                                      {"bookId": bookId, "sectionId": section_id})
        else:
            content = get_xml_content(project, "est", filename, xsl_file, {"bookId": bookId})

        data = {
            # @TODO: investigate if id should have language in its value or not (similar to filename).
            "id": "{}_{}_est".format(collection_id, publication_id),
            "content": content
        }
        if language is not None:
            data["language"] = language
        return jsonify(data), 200
    else:
        return jsonify({
            "id": "{}_{}".format(collection_id, publication_id),
            "error": message
        }), 403
