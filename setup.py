from setuptools import setup

setup(
    name='sls_api',
    packages=['sls_api'],
    version="0.0.1",
    include_package_data=True,
    install_requires=[
        'flask==0.12.*',
        'lxml==4.1.*',
        'PyMySQL==0.8.* ',
        'python-dateutil==2.6.*',
        'PyYAML==3.12',
        'requests==2.18.*'
    ]
)
