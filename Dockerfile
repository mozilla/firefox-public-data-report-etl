FROM python:3.13-bookworm

# add a non-privileged user for running the application
RUN groupadd --gid 10001 app && \
    useradd -g app --uid 10001 --shell /usr/sbin/nologin --create-home --home-dir /app app

WORKDIR /app

ADD ./requirements-dev.txt /app/requirements-dev.txt

RUN pip install --upgrade pip && pip install -r requirements-dev.txt

ADD ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ADD . /app

RUN chown -R 10001:10001 /app


ENTRYPOINT ["/usr/local/bin/python"]
