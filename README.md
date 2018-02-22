# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python 2.7
- Unified API for all publically accessible sls.fi data (eventually)
- Installation details:
    - Create config files from _example files in `config` folder
    - If needed, tweak `uwsgi.ini` file to adjust uWSGI server settings
    - run `docker build -t sls_api .` in root folder containing `Dockerfile` and `uwsgi.ini`
    
- Running in Production
    - Start api using `docker run -p 80:80 --name=sls_api --restart=always sls_api`
    - (if needed, other ports can be used instead, i.e. `-p 8080:80`)
    
- Manually testing the API without Docker, using a python virtualenv
    - `source /path/to/virtualenv/bin/activate`
    - `pip install -e .`
    - `export FLASK_APP=/path/to/sls_api`
    - `flask run`
    
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
 
### /digitaleditions endpoint
- Port of older PHP apis:
    - https://github.com/slsfi/digital_editions_API 
    - https://github.com/slsfi/digital_editions_xslt
- Needs connection details for MySQL server and paths to folders for XML, HTML, and XSL files
    - configs/digital_editions.yml
    - See digital_editions_example.yml for specifics

### /filemaker endpoint
- Direct link to a Filemaker Server 2016 REST API
- https://fmhelp.filemaker.com/docs/16/en/restapi/
- Base URL for Filemaker Server defined in configs/filemaker.yml
    - see configs/filemaker_example.yml
