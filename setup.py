from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.2.0",
    include_package_data=True,
    install_requires=[
        'argon2_cffi~=21.3.0',
        'beautifulsoup4~=4.10.0',
        'elasticsearch~=7.16.2',
        'flask~=2.0.2',
        'flask-jwt-extended~=4.3.1',
        'flask-sqlalchemy~=2.5.1',
        'flask-cors~=3.0.10',
        'lxml~=4.7.1',
        'mysqlclient~=2.1.0',
        'passlib~=1.7.4',
        'psycopg2-binary~=2.9.2',
        'raven[flask]~=6.10.0',
        'ruamel.yaml~=0.17.19',
        'requests~=2.26.0',
        'sqlalchemy~=1.4.29',
        'Pillow~=8.4.0',
        'werkzeug~=2.0.2'
    ]
)
