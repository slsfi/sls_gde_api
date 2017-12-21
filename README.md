# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python 2.7
- Unified API for all publically accessible sls.fi data (eventually)
- Install into /var/sites/sls_api/
    - Use sls_api.conf for Apache2 sites-available config
    - Install using Python 2.7 virtualenv at /home/webapp/virtualenvs/sls_api
    
### /images endpoint
- Provides image accessfiles through Isilon Swift
- Needs a list of allowed accessfiles and swift authentication details to work
    - configs/derivate_objects_list.txt and configs/swift_auth.yml
    - See _example files for more specific details
    
### /oai endpoint
- Provides OAI-PMH metadata in XML format for Finna and Europeana
- Follows https://www.openarchives.org/OAI/openarchivesprotocol.html
- Needs connection details to MySQL server housing metadata
    - configs/mysql.yml
    - See mysql_example.yml for specifics
    
