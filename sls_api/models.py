from flask_sqlalchemy import SQLAlchemy
from passlib.context import CryptContext


pwd_context = CryptContext(
    schemes=["argon2", "pbkdf2_sha512", "pbkdf2_sha256"],
    deprecated="auto"
)

db = SQLAlchemy()


class User(db.Model):
    __tablename__ = 'users'

    ident = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.Unicode(255), unique=True, nullable=False)
    password = db.Column(db.UnicodeText, nullable=False)
    projects = db.Column(db.UnicodeText, nullable=True, comment="Comma-separated list of projects this user has edit rights to")

    def save_to_db(self):
        hashed_password = pwd_context.hash(self.password)
        self.password = hashed_password
        db.session.add(self)
        db.session.commit()

    def get_projects(self):
        if self.projects:
            return self.projects.split(",")
        return None

    def get_token_identity(self):
        return {
            "sub": self.email,
            "projects": self.get_projects()
        }

    @classmethod
    def find_by_email(cls, email):
        return cls.query.filter_by(email=email).first()

    @staticmethod
    def verify_password_hash(password, stored_hash):
        return pwd_context.verify(password, stored_hash)
