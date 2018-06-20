from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="0.0.1",
    include_package_data=True,
    install_requires=[
        'argon2_cffi',
        'flask',
        'flask-jwt-extended',
        'flask-sqlalchemy',
        'flask-sslify',
        'flasgger',
        'lxml',
        'mysqlclient',
        'passlib',
        'psycopg2',
        'raven[flask]',
        'ruamel.yaml',
        'requests',
        'sqlalchemy',
        'werkzeug'
    ]
)
