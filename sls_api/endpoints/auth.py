from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt_identity, jwt_required
from sls_api.models import User


auth = Blueprint('auth', __name__)

"""
JWT-based Authorization

Routes in the API protected by @jwt_required can only be accessed with a proper JWT token in the header
JWT Header format is "Authorization: Bearer <JWT_TOKEN>"
"""


@auth.route("/register", methods=["POST"])
def register_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No JSON in payload."}), 400

    email = data.get("email", None)
    password = data.get("password", None)

    if not email or not password:
        return jsonify({"msg": "email or password not in JSON payload."}), 400
    if User.find_by_email(data["email"]):
        return jsonify({"msg": "User {!r} already exists.".format(data["email"])}), 400

    try:
        new_user = User.create_new_user(email, password)
        identity = new_user.get_token_identity()
        return jsonify(
            {
                "msg": "User {!r} was created. Contact support to be given editing rights for GDE projects.".format(data["email"]),
                "access_token": create_access_token(identity=identity),
                "refresh_token": create_refresh_token(identity=identity)
            }
        ), 201
    except Exception:
        return jsonify({"msg": "Error in user registration"}), 500


@auth.route("/login", methods=["POST"])
def login_user():
    data = request.get_json()
    if not data:
        return jsonify({"msg": "No credentials provided."}), 400

    email = data.get("email", None)
    password = data.get("password", None)
    current_user = User.find_by_email(email)
    try:
        success = current_user.check_password(password)
    except Exception:
        return jsonify({"msg": "Incorrect email or password."}), 400
    if not success:
        return jsonify({"msg": "Incorrect email or password."}), 400

    identity = current_user.get_token_identity()

    return jsonify(
        {
            "msg": "Logged in as {!r}".format(data["email"]),
            "access_token": create_access_token(identity=identity),
            "refresh_token": create_refresh_token(identity=identity),
            "user_projects": current_user.get_projects()
        }
    ), 200


@auth.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh_token():
    identity = get_jwt_identity()
    return jsonify(
        {
            "msg": "Logged in as {!r}".format(identity["sub"]),
            "access_token": create_access_token(identity=identity)
        }
    ), 200


@auth.route("/test", methods=["POST"])
@jwt_required()
def test_authentication():
    return jsonify(get_jwt_identity())
