# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python >=3.6
- Unified API for all publically accessible sls.fi data (eventually)
- Installation details:
    - Create config files from _example files in `config` folder
    - Ensure volume paths in `docker-compose.yml` point at the correct host and container folders
    - If needed, tweak `uwsgi.ini` file to adjust uWSGI server settings
    - run `docker-compose build` in root folder containing `Dockerfile`, `uwsgi.ini`, and `docker-compose.yml`
    
- Running in Production
    - Start api using `docker-compose up -d`
    - Please note that the default port is 80, this can be changed in `docker-compose.yml`
    
- Manually testing the API without Docker, using a python virtualenv
    - `source /path/to/virtualenv/bin/activate`
    - `pip install -e .`
    - `export FLASK_APP=/path/to/sls_api`
    - `flask run`
    
### /accessfiles endpoint
- Provides accessfiles through Isilon Swift for Finna and Europeana
- Needs a list of allowed accessfiles and swift authentication details to work
    - configs/swift_file_list.txt and configs/swift_auth.yml
    - See _example files for more specific details
- List of allowed files can be generated from FileMaker .csv export using
    - https://bitbucket.org/rasek_sls/accessfile_list_generator  (private repo)
- swift_file_list.txt should be a newline-separated list of valid filepaths inside the Swift home directory
    - In other words, filepaths relative to the home directory of the swift user configured in swift_auth.yml
    
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
