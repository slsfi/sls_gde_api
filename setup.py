from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.3.0",
    include_package_data=True,
    install_requires=[
        'argon2_cffi~=23.1.0',
        'beautifulsoup4~=4.12.2',
        'elasticsearch==7.13.4',
        'flask~=2.3.3',
        'flask-jwt-extended~=4.5.2',
        'flask-sqlalchemy~=3.0.5',
        'flask-cors~=4.0.0',
        'lxml~=4.9.3',
        'mysqlclient~=2.2.0',
        'passlib~=1.7.4',
        'psycopg2-binary~=2.9.7',
        'raven[flask]~=6.10.0',
        'ruamel.yaml~=0.17.32',
        'requests~=2.31.0',
        'sqlalchemy~=2.0.20',
        'Pillow~=10.0.0',
        'werkzeug~=2.3.7'
    ]
)
