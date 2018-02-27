from __future__ import unicode_literals
from flask import Flask
import os
app = Flask(__name__)

# Selectively import and register endpoints based on which configs exist and can be loaded
if os.path.exists(os.path.join("sls_api", "configs", "digital_editions.yml")):
    from endpoints.digital_editions import digital_edition
    app.register_blueprint(digital_edition, url_prefix="/digitaledition")
if os.path.exists(os.path.join("sls_api", "configs", "filemaker.yml")):
    from endpoints.filemaker import filemaker
    app.register_blueprint(filemaker, url_prefix="/filemaker")
if os.path.exists(os.path.join("sls_api", "configs", "oai.yml")):
    from endpoints.oai import oai
    app.register_blueprint(oai, url_prefix="/oai")
if os.path.exists(os.path.join("sls_api", "configs", "swift_auth.yml")):
    from endpoints.swift_accessfiles import swift
    app.register_blueprint(swift, url_prefix="/accessfiles")

print(" * Loaded endpoints: {}".format(", ".join(app.blueprints)))
