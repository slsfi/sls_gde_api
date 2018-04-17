# Unified REST API for sls.fi (api.sls.fi)
- Flask-driven REST API for sls.fi
- Runs on Python >=3.6 (python2 may work, but will likely run slower and may have issues with unicode)
- Unified API for all publically accessible sls.fi data 
---
Copyright 2018 Svenska Litteratursällskapet i Finland, r.f.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
---
- For full details on licenses in-use by the SLS API and its components, see `LICENSES.txt`

- Installation details:
    - Create config files from _example files in `config` folder
    - Ensure volume paths in `docker-compose.yml` point at the correct host and container folders
    - run `docker-compose build` in root folder containing `Dockerfile` and `docker-compose.yml`
    
- Running in Production
    - Start api using `docker-compose up -d`
    - Please note that the default port is 8080, this can be changed in `docker-compose.yml`
    
- Manually testing the API without Docker, using a python virtualenv
    - `source /path/to/virtualenv/bin/activate` or `/path/to/virtualenv/Scripts/activate_this.bat` on Windows
    - `pip install -e .`
    - `export FLASK_APP=/path/to/sls_api` or `set FLASK_APP=/path/to/sls_api` on Windows
    - `export FLASK_DEBUG=1` or `set FLASK_DEBUG=1`on Windows to activate DEBUG mode
    - `flask run` - note that this uses port 5000 by default
    - These commands are contained within the `start-standalone.sh` script, so you can start the API by running `./start-standalone.sh` on UNIX-like systems.

### /apidocs endpoint
- Provides Swagger UI API documentation
- Documentation also available as OpenAPI 3.0.0 spec, see `openapi.json`

### /accessfiles endpoint
- Provides accessfiles through Isilon Swift for Finna and Europeana
- Needs a list of allowed accessfiles and swift authentication details to work
    - configs/swift_file_list.txt and configs/swift_auth.yml
    - See _example files for more specific details
- List of allowed files can be generated from FileMaker .csv export using
    - https://bitbucket.org/rasek_sls/accessfile_list_generator  (private repo)
- swift_file_list.txt should be a newline-separated list of valid filepaths inside the Swift home directory
    - In other words, filepaths relative to the home directory of the swift user configured in swift_auth.yml

### /digitaleditions endpoint
- Endpoints used for the SLS Generic Digital Edition platform
- Currently made for the SLS metadata database, being re-made for the GDE 2018 database spec, see branch `gde2018-database-spec`
- Port of older PHP apis:
    - https://github.com/slsfi/digital_editions_API 
    - https://github.com/slsfi/digital_editions_xslt
- Needs connection details for MySQL server and paths to folders for XML, HTML, and XSL files
    - configs/digital_editions.yml
    - See digital_editions_example.yml for specifics    

### /oai endpoint
- Provides OAI-PMH metadata in XML format for Finna and Europeana
- Follows https://www.openarchives.org/OAI/openarchivesprotocol.html
- Needs connection details to MySQL server housing metadata
- Currently made for the SLS metadata database, specification for this TBD
    - configs/oai.yml
    - See oai_example.yml for specifics
