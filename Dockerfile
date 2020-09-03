FROM python:3.8

COPY requirements.txt /
RUN set -ex && \
    pip install -r /requirements.txt
EXPOSE 8080
COPY ./app /transit-occupancy-dashboard

WORKDIR /transit-occupancy-dashboard

CMD exec gunicorn -b :8080 transitapp:server --timeout 1800
