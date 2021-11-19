import argparse
import sys

from sls_api.models import User


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Helper script to reset a User password")
    parser.add_argument("email", help="User email address")
    parser.add_argument("new_password", help="New user password")

    args = parser.parse_args()

    success = User.reset_password(args.email, args.new_password)
    if success is None:
        print("Error during password reset! Check API backend logs.")
        sys.exit(1)
    elif success:
        print(f"Password for user {args.email} successfully changed to {args.new_password}!")
        sys.exit(0)
    else:
        print(f"No user with the email {args.email} could be found!")
        sys.exit(1)
