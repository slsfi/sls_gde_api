from __future__ import unicode_literals
from flask import Flask
from endpoints.digital_editions import digital_edition
from endpoints.oai import oai
from endpoints.swift_accessfiles import swift
app = Flask(__name__)

# Register API endpoints here
app.register_blueprint(digital_edition, url_prefix="/digitaledition")
app.register_blueprint(oai, url_prefix="/oai")
app.register_blueprint(swift, url_prefix="/accessfiles")
