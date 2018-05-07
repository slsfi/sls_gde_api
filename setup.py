from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="0.0.1",
    include_package_data=True,
    install_requires=[
        'flask==1.*',
        'flasgger==0.8.*',
        'lxml==4.2.*',
        'mysqlclient==1.3.*',
        'psycopg2==2.7.*',
        'ruamel.yaml==0.15.*',
        'requests==2.18.*',
        'sqlalchemy==1.2.*'
    ]
)
