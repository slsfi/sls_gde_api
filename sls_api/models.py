from passlib.context import CryptContext
from sls_api import db


pwd_context = CryptContext(
    schemes=["argon2", "pbkdf2_sha512", "pbkdf2_sha256"],
    deprecated="auto"
)


class User(db.Model):
    __tablename__ = 'users'

    ident = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    password = db.Column(db.Text, nullable=False)
    projects = db.Column(db.UnicodeText, nullable=True, comment="Comma-separated list of projects this user has edit rights to")

    def save_to_db(self):
        hashed_password = pwd_context.hash(self.password)
        self.password = hashed_password
        db.session.add(self)
        db.session.commit()

    def get_projects(self):
        return self.projects.split(",")

    @classmethod
    def find_by_email(cls, email):
        return cls.query.filter_by(email=email).first()

    @staticmethod
    def verify_password_hash(password, stored_hash):
        return pwd_context.verify(password, stored_hash)
