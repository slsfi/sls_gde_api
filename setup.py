from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="0.0.2",
    include_package_data=True,
    install_requires=[
        'argon2_cffi',
        'flask',
        'flask-jwt-extended',
        'flask-sqlalchemy',
        'flask-cors',
        'flasgger',
        'lxml',
        'mysqlclient',
        'passlib',
        'psycopg2',
        'raven[flask]',
        'ruamel.yaml',
        'requests',
        'simplejson',
        'sqlalchemy',
        'Pillow',
        'werkzeug',
        'elasticsearch'
    ]
)
