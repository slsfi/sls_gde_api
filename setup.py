from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.2.0",
    include_package_data=True,
    install_requires=[
        'argon2_cffi~=21.3.0',
        'beautifulsoup4~=4.11.1',
        'elasticsearch==7.13.4',
        'flask~=2.2.2',
        'flask-jwt-extended~=4.4.4',
        'flask-sqlalchemy~=2.5.1',
        'flask-cors~=3.0.10',
        'lxml~=4.9.1',
        'mysqlclient~=2.1.1',
        'passlib~=1.7.4',
        'psycopg2-binary~=2.9.4',
        'raven[flask]~=6.10.0',
        'ruamel.yaml~=0.17.21',
        'requests~=2.28.1',
        'sqlalchemy~=1.4.41',
        'Pillow~=9.2.0',
        'werkzeug~=2.2.2'
    ]
)
