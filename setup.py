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
        'PyMySQL==0.8.* ',
        'python-dateutil==2.7.*',
        'ruamel.yaml==0.15.*',
        'requests==2.18.*'
    ]
)
