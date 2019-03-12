FROM python:3.6-slim-stretch

RUN apt update && apt install -y build-essential libmariadbclient-dev

RUN useradd -ms /bin/bash uwsgi
RUN mkdir /app
RUN chown -R uwsgi /app
USER uwsgi

WORKDIR /app
COPY . /app/

USER root
RUN pip install uwsgi
RUN pip install -e .

USER uwsgi
# Add SSH configuration to ssh config file for openssh agent
RUN mkdir ~/.ssh
RUN cat ssh_config >> ~/.ssh/config
RUN cp /app/ssh/* ~/.ssh/

CMD ["uwsgi", \
     "--socket", "0.0.0.0:3031", \
     "--uid", "uwsgi", \
     "--plugins", "python3", \
     "--protocol", "uwsgi", \
     "--wsgi", "sls_api:app", \
     "--master", \
     "--enable-threads", \
     "--processes", "1", \
     "--workers", "4", \
     "--reload-on-rss", "500" \
]
