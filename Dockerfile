FROM python:3.7
ENV DASH_DEBUG_MODE True

COPY requirements.txt /
RUN set -ex && \
    pip install -r /requirements.txt
EXPOSE 8080
COPY ./ /transit-occupancy-dashboard

WORKDIR /transit-occupancy-dashboard

CMD ["python", "transitapp.py"]
