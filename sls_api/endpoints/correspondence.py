from flask import Blueprint, jsonify
import logging
from sqlalchemy.sql import text

from sls_api.endpoints.generics import db_engine, get_project_id_from_name

correspondence = Blueprint('correspondence', __name__)
logger = logging.getLogger("sls_api.correspondence")

# Work register metadata functions


@correspondence.route("/<project>/correspondence/publication/metadata/<pub_id>")
def get_correspondence_metadata_for_publication(project, pub_id):
    logger.info("Getting results for /correspondence/manifestations/")
    connection = db_engine.connect()
    project_id = get_project_id_from_name(project)
    corresp_sql = """SELECT c.*, ec.type,s.full_name as full_name from publication p
                    join correspondence c on concat('Br', c.legacy_id) = substring(p.original_filename, 'Br[0-9]{1,5}')
                    join event_connection ec on ec.correspondence_id = c.id
                    join subject s on s.id = ec.subject_id
                    where p.id = :pub_id and c.project_id = :p_id """
    corresp_sql = text(corresp_sql).bindparams(pub_id=pub_id, p_id=project_id)
    corresp = []
    subjects = []
    for row in connection.execute(corresp_sql).fetchall():
        subject = {}
        letter = {}
        subject[row['type']] = row['full_name']
        letter[row['id']] = dict(row)
        subjects.append(dict(subject))
        corresp.append(dict(letter))

    data = {
        'letter': dict(corresp[0]),
        'subjects': subjects
    }
    connection.close()
    return jsonify(data)
