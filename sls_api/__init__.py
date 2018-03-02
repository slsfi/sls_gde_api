from flask import Flask
import logging
import os
from sys import stdout
app = Flask(__name__)

root_logger = logging.getLogger()
if int(os.environ.get("FLASK_DEBUG", 0)) == 1:
    root_logger.setLevel(logging.DEBUG)
else:
    root_logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(stream=stdout)
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%H:%M:%S'))
root_logger.addHandler(stream_handler)

logger = logging.getLogger("sls_api")

# Selectively import and register endpoints based on which configs exist and can be loaded
if os.path.exists(os.path.join("sls_api", "configs", "digital_editions.yml")):
    from sls_api.endpoints.digital_editions import digital_edition
    app.register_blueprint(digital_edition, url_prefix="/digitaledition")
if os.path.exists(os.path.join("sls_api", "configs", "filemaker.yml")):
    from sls_api.endpoints.filemaker import filemaker
    app.register_blueprint(filemaker, url_prefix="/filemaker")
if os.path.exists(os.path.join("sls_api", "configs", "oai.yml")):
    from sls_api.endpoints.oai import oai
    app.register_blueprint(oai, url_prefix="/oai")
if os.path.exists(os.path.join("sls_api", "configs", "swift_auth.yml")):
    from sls_api.endpoints.swift_accessfiles import swift
    app.register_blueprint(swift, url_prefix="/accessfiles")

logger.info(" * Loaded endpoints: {}".format(", ".join(app.blueprints)))
