FROM tiangolo/uwsgi-nginx-flask:python2.7

WORKDIR /app
RUN rm -rf /app
ADD . /app

RUN pip install -e .
