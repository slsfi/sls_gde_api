from flask import Blueprint, jsonify
import logging
from sqlalchemy.sql import text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name

workregister = Blueprint('workregister', __name__)
logger = logging.getLogger("sls_api.workregister")

# Work register metadata functions


@workregister.route("/<project>/workregister/manifestations/")
def get_work_manifestations_for_project(project):
    logger.info("Getting results for /workregister/manifestations/")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    manifestation_sql = "SELECT json_data FROM get_manifestations_with_authors WHERE project_id = :proj_id"
    manifestation_sql = text(manifestation_sql).bindparams(proj_id=project_id)

    manifestations = []
    result = connection.execute(manifestation_sql)
    row = result.fetchone()
    while row is not None:
        manifestations.append(row)
        row = result.fetchone()

    connection.close()

    return jsonify(manifestations)


@workregister.route("/<project>/workregister/manifestation/authors/<manifestation_id>")
def get_work_manifestation_authors_for_project(project, manifestation_id):
    logger.info("Getting results for /workregister/manifestation/authors/<manifestation_id>")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    # Only allow int for manifestation_id
    if isinstance(manifestation_id, int) is False:
        connection.close()
        return jsonify({"msg": "Desired manifestation_id not found in database."}), 404

    authors_sql = "SELECT s.* FROM work_manifestation w_m " \
                  "JOIN event_connection e_c ON e_c.work_manifestation_id = w_m.id " \
                  "JOIN subject s ON s.id = e_c.subject_id " \
                  "JOIN event e ON e.id = e_c.event_id " \
                  "JOIN work_reference w_r ON w_r.work_manifestation_id = w_m.id " \
                  "WHERE w_m.id = :mani_id " \
                  "AND e_c.deleted = 0 AND e.deleted = 0 AND w_r.deleted = 0 AND w_m.deleted = 0 AND s.deleted = 0 " \
                  "AND w_r.project_id = :proj_id " \
                  "ORDER BY s.last_name, s.full_name"

    authors_sql = text(authors_sql).bindparams(mani_id=manifestation_id, proj_id=project_id)

    authors = []
    result = connection.execute(authors_sql)
    row = result.fetchone()
    while row is not None:
        authors.append(dict(row))
        row = result.fetchone()

    connection.close()

    return jsonify(authors)


@workregister.route("/<project>/workregister/manifestation/occurrences/<manifestation_id>")
def get_work_manifestation_occurrences_for_project(project, manifestation_id):
    logger.info("Getting results for /workregister/manifestation/occurrences/<manifestation_id>")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    # Only allow int for manifestation_id
    if isinstance(manifestation_id, int) is False:
        connection.close()
        return jsonify({"msg": "Desired manifestation_id not found in database."}), 404

    occurrences_sql = "SELECT p.* FROM work_manifestation w_m " \
                      "JOIN event_connection e_c ON e_c.work_manifestation_id = w_m.id " \
                      "JOIN event e ON e.id = e_c.event_id " \
                      "JOIN event_occurrence e_o ON e.id = e_o.event_id " \
                      "JOIN work_reference w_r ON w_r.work_manifestation_id = w_m.id " \
                      "JOIN publication p ON p.id = e_o.publication_id " \
                      "WHERE w_m.id = :mani_id " \
                      "AND e_c.deleted = 0 AND e.deleted = 0 AND w_r.deleted = 0 AND p.deleted = 0 AND w_m.deleted = 0 AND e_o.deleted = 0 " \
                      "AND w_r.project_id = :proj_id"

    occurrences_sql = text(occurrences_sql).bindparams(mani_id=manifestation_id, proj_id=project_id)

    occurrences = []
    result = connection.execute(occurrences_sql)
    row = result.fetchone()
    while row is not None:
        occurrences.append(dict(row))
        row = result.fetchone()

    connection.close()

    return jsonify(occurrences)
