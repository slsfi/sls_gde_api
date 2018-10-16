# REST API for SLS Generic Digital Editions project
- Flask-driven REST API
- Runs on Python 3.6 (Python2 may work, but will likely run slower and may have issues with unicode)
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
- For full license text, see `LICENSE` file.

- Installation details:
    - Create config files from _example files in `config` folder
    - Ensure volume paths in `docker-compose.yml` point at the correct host and container folders
    - Production-ready steps:
        - Add a valid SSL certificate as `ssl/cert.pem` and a matching private key as `ssl/key.pem`
        - Configure SSH for Git repositories in `ssh_config` and add identity files matching it into `ssh` folder.
    - run `docker-compose build` in root folder containing `Dockerfile` and `docker-compose.yml`
    
- Running in Production
    - Start api using `docker-compose up -d`
    - Please note that the default port is 8000, this can be changed in `docker-compose.yml`
    - API can then be accessed at http://127.0.0.1:8000 OR https://127.0.0.1:8000 if SSL was enabled
    
- Manually testing the API without Docker, using a python virtualenv
    - `source /path/to/virtualenv/bin/activate` or `/path/to/virtualenv/Scripts/activate_this.bat` on Windows
    - `pip install --upgrade -e .`
    - `export FLASK_APP=/path/to/sls_api` or `set FLASK_APP=/path/to/sls_api` on Windows
    - `export FLASK_DEBUG=1` or `set FLASK_DEBUG=1`on Windows to activate DEBUG mode
    - By using the user `test@test.com` with the password `test`, access to all projects in granted in DEBUG mode
    - `flask run` - note that this uses port 5000 by default
    - These commands are contained within the `start-standalone.sh` script, so you can start the API by running `./start-standalone.sh` on UNIX-like systems.

### /apidocs endpoint
- Provides Swagger UI API documentation
- Documentation also available as OpenAPI 3.0.0 spec, see `openapi.json`

### /auth endpoint
- Enables JWT-based authentication towards protected endpoints
- Provides registration, login, and token refresh for users

### /digitaleditions endpoint
- Endpoints used for the SLS Generic Digital Edition platform
- Currently made for the SLS metadata database, being re-made for the GDE 2018 database spec, see branch `gde2018-database-spec`
- Port of older PHP apis:
    - https://github.com/slsfi/digital_editions_API 
    - https://github.com/slsfi/digital_editions_xslt
- Needs connection details for MySQL server and paths to folders for XML, HTML, and XSL files
    - configs/digital_editions.yml
    - See digital_editions_example.yml for specifics    
