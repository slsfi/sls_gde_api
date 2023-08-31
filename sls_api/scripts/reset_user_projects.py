import argparse
import sys

from sls_api.models import User
from sls_api import app

if __name__ == "__main__":
    with app.app_context():
        parser = argparse.ArgumentParser(description="Helper script to reset a Users projects")
        parser.add_argument("email", help="User email address")
        parser.add_argument("projects", help="User projects")

        args = parser.parse_args()

        success = User.reset_projects(args.email, args.projects)
        if success is None:
            print("Error during projects reset! Check API backend logs.")
            sys.exit(1)
        elif success:
            print(f"Projects for user {args.email} successfully changed to {args.projects}!")
            sys.exit(0)
        else:
            print(f"No user with the email {args.email} could be found!")
            sys.exit(1)
