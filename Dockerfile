FROM python:3.7-buster

# add a non-privileged user for running the application
RUN groupadd --gid 10001 app && \
    useradd -g app --uid 10001 --shell /usr/sbin/nologin --create-home --home-dir /app app

WORKDIR /app

# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jre

RUN pip install --upgrade pip && pip install tox setuptools wheel flake8

ADD ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ADD . /app

RUN chown -R 10001:10001 /app


ENTRYPOINT ["/usr/local/bin/python"]
