# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python 2.7
- Unified API for all publically accessible sls.fi data (eventually)
- Install into /var/sites/sls_api/
    - Use sls_api.conf for Apache2 sites-available config
    - Install using Python 2.7 virtualenv at /home/webapp/virtualenvs/sls_api
    
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
    - configs/mysql.yml
    - See mysql_example.yml for specifics
    
