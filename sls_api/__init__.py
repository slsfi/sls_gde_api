from flask import Flask, redirect, url_for
from flask_jwt_extended import JWTManager
from flasgger import Swagger
from flask_cors import CORS
import logging
import json
import os
from raven.contrib.flask import Sentry
from ruamel.yaml import YAML
from sys import stdout

app = Flask(__name__)
CORS(app)
yaml = YAML(typ="safe")

# First, set up logging
root_logger = logging.getLogger()
if int(os.environ.get("FLASK_DEBUG", 0)) == 1:
    root_logger.setLevel(logging.DEBUG)
else:
    root_logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler(stream=stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
root_logger.addHandler(stream_handler)

logger = logging.getLogger("sls_api")

# if there's a sentry.io config file, enable error tracking towards it
if os.path.exists(os.path.join("sls_api", "configs", "sentry.yml")):
    with open(os.path.join("sls_api", "configs", "sentry.yml")) as config_file:
        sentry_config = yaml.load(config_file.read())
    if "sentry_dsn" in sentry_config and sentry_config["sentry_dsn"] != "":
        logger.info("Enabled sentry.io error tracking with settings from 'sentry.yml'")
        sentry = Sentry(app, dsn=sentry_config["sentry_dsn"], logging=True, level=logging.ERROR)
    else:
        sentry = None
else:
    sentry = None

# Load in Swagger UI to display api documentation at /apidocs, with a 302-redirect at the base URL
if os.path.exists("openapi.json"):
    app.config["SWAGGER"] = {
        "title": "SLS API",
        "uiversion": 3  # Use the Swagger 3.* UI, to properly support OpenAPI 3.0.0
    }
    with open("openapi.json") as json_file:
        swagger = Swagger(app, template=json.load(json_file))

    # redirect requests at the base URL to /apidocs
    @app.route("/")
    def redir_to_docs():
        return redirect(url_for("flasgger.apidocs"), code=302)
else:
    logger.warning("Could not load openapi.json specification file!")


# Selectively import and register endpoints based on which configs exist and can be loaded
if os.path.exists(os.path.join("sls_api", "configs", "digital_editions.yml")):
    from sls_api.endpoints.digital_editions import digital_edition
    app.register_blueprint(digital_edition, url_prefix="/digitaledition")

if os.path.exists(os.path.join("sls_api", "configs", "security.yml")):
    if not os.path.exists(os.path.join("ssl", "cert.pem")) and not os.path.exists(os.path.join("ssl", "key.pem")) and not int(os.environ.get("FLASK_DEBUG", 0)) == 1:
        logger.error("No SSL certificate was found, disabling JWT authorization and protected endpoints.")
    else:
        from sls_api.endpoints.auth import auth
        from sls_api.models import db, User
        with open(os.path.join("sls_api", "configs", "security.yml")) as config_file:
            security_config = yaml.load(config_file.read())
        app.config["SECRET_KEY"] = security_config["secret_key"]
        app.config["SQLALCHEMY_DATABASE_URI"] = security_config["user_database"]
        app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

        jwt = JWTManager(app)
        db.init_app(app)

        app.register_blueprint(auth, url_prefix="/auth")

        @app.before_first_request
        def create_tables():
            db.create_all()
            if not User.find_by_email("test@test.com"):
                User.create_new_user("test@test.com", "test")

if "digital_edition" in app.blueprints and "auth" in app.blueprints:
    """
    If we have a digital_edition config, and JWT is configured, load in JWT-protected publication tool endpoints
    """
    from sls_api.endpoints.tools_events import event_tools
    app.register_blueprint(event_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools_publications import publication_tools
    app.register_blueprint(publication_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools_files import file_tools
    app.register_blueprint(file_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools_collections import collection_tools
    app.register_blueprint(collection_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools_groups import group_tools
    app.register_blueprint(group_tools, url_prefix="/digitaledition")
    from sls_api.endpoints.tools_publishing import publishing_tools
    app.register_blueprint(publishing_tools, url_prefix="/digitaledition")

logger.info(" * Loaded endpoints: {}".format(", ".join(app.blueprints)))
