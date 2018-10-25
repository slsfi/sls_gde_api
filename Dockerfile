FROM python:3.6-slim-stretch

RUN apt update && apt install -y build-essential libmariadbclient-dev subversion

RUN pip install Twisted[tls]

# Make folders and copy in all code
RUN mkdir /app && mkdir /log && mkdir /log/digital_editions
WORKDIR /app
ADD . /app

# Add SSH configuration to ssh config file for openssh agent
#RUN cat ssh_config >> ~/.ssh/config
#RUN cp /app/ssh/* ~/.ssh/

# Install sls_api package
RUN pip install -e .

CMD ["./start-container.sh"]
