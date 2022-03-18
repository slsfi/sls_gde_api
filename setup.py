from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="1.2.0",
    include_package_data=True,
    install_requires=[
        'argon2_cffi==20.*',
        'beautifulsoup4==4.9.*',
        'flask==1.1.*',
        'flask-jwt-extended==3.*',
        'flask-sqlalchemy==2.4.*',
        'flask-cors==3.0.*',
        'flasgger==0.9.*',
        'lxml==4.6.*',
        'mysqlclient==2.0.*',
        'passlib==1.7.*',
        'psycopg2-binary==2.9.*',
        'raven[flask]==6.10.*',
        'ruamel.yaml==0.16.*',
        'requests==2.25.*',
        'simplejson==3.17.*',
        'sqlalchemy==1.3.*',
        'Pillow==8.3.*,>=8.3.2',
        'werkzeug==1.0.*',
        'elasticsearch==7.11.*'
    ]
)
