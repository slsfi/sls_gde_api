from flask import Flask, redirect, url_for
from flask_jwt_extended import JWTManager
from flask_sqlalchemy import SQLAlchemy
from flasgger import Swagger
import logging
import json
import os
from ruamel.yaml import YAML
from sys import stdout

app = Flask(__name__)

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
    from sls_api.endpoints.auth import auth
    yaml = YAML()
    with open(os.path.join("sls_api", "configs", "security.yml")) as config_file:
        security_config = yaml.load(config_file.read())
    app.config["JWT_SECRET_KEY"] = security_config["secret_key"]
    app.config["SQLALCHEMY_DATABASE_URI"] = security_config["user_database"]

    jwt = JWTManager(app)
    db = SQLAlchemy(app)
    app.register_blueprint(auth, url_prefix="/auth")

    @app.before_first_request
    def create_tables():
        db.create_all()

logger.info(" * Loaded endpoints: {}".format(", ".join(app.blueprints)))
