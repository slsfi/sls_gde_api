from flask import Blueprint, jsonify, request
from flask_jwt_extended import create_access_token, create_refresh_token, get_jwt_identity, jwt_refresh_token_required
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
        new_user = User(
            email=data["email"],
            password=data["password"]
        )
        new_user.save_to_db()
        identity = new_user.get_token_identity()
        return jsonify(
            {
                "msg": "User {!r} was created. Contact support to be given editing rights for GDE projects.".format(data["email"]),
                "access_token": create_access_token(identity=identity),
                "refresh_token": create_refresh_token(identity=identity)

            }
        ), 200
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
        success = User.verify_password_hash(password, current_user.password)
    except Exception:
        return jsonify({"msg": "Incorrect email or password."}), 400
    if not success:
        return jsonify({"msg": "Incorrect email or password."}), 400

    identity = current_user.get_token_identity()

    return jsonify(
        {
            "msg": "Logged in as {!r}".format(data["email"]),
            "access_token": create_access_token(identity=identity),
            "refresh_token": create_refresh_token(identity=identity)
        }
    )


@auth.route("/refresh", methods=["POST"])
@jwt_refresh_token_required
def refresh_token():
    identity = get_jwt_identity()
    access_token = create_access_token(identity=identity)
    return jsonify(
        {
            "msg": "Logged in as {!r}".format(identity["email"]),
            "access_token": access_token
        }
    )
