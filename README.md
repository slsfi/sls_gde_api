# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python 2.7
- Unified API for all publically accessible sls.fi data (eventually)
- Installation details:
    - Setup a virtualenv
    - `source /path/to/created/venv/bin/activate`
    - `pip install -e .`
    - `export FLASK_APP=/path/to/sls_api`
    - http://flask.pocoo.org/docs/0.12/patterns/packages/
- Running the API
    - Install `apache2` and `mod_wsgi`
    - Edit sls_api_example.conf, change `/path/to/sls_api` to actual path to sls_api folder
    - Save new file as `/etc/apache2/sites-available/sls_api.conf`
    - Run `a2ensite sls_api.conf`
    - Run `apachectl graceful`
    
### /accessfiles endpoint
- Provides accessfiles through Isilon Swift for Finna and Europeana
- Needs a list of allowed accessfiles and swift authentication details to work
    - configs/derivate_objects_list.txt and configs/swift_auth.yml
    - See _example files for more specific details
- List of accessfiles can be generated from FileMaker .csv export using
    - https://bitbucket.org/rasek_sls/accessfile_list_generator
    
### /oai endpoint
- Provides OAI-PMH metadata in XML format for Finna and Europeana
- Follows https://www.openarchives.org/OAI/openarchivesprotocol.html
- Needs connection details to MySQL server housing metadata
    - configs/oai.yml
    - See oai_example.yml for specifics
- Uses a C-based MySQL python connector library, mysqlclient
    - https://github.com/PyMySQL/mysqlclient-python/blob/master/README.md
    - Difficult to install on Windows, pymysql can be used instead, with ~0.5x performance
 
### /digitaleditions endpoint
- Port of older PHP apis:
    - https://github.com/slsfi/digital_editions_API 
    - https://github.com/slsfi/digital_editions_xslt
 