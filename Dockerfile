FROM ubuntu:18.04

# add a non-privileged user for running the application
RUN groupadd --gid 10001 app && \
    useradd -g app --uid 10001 --shell /usr/sbin/nologin --create-home --home-dir /app app

WORKDIR /app

# Install python
RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && cd /usr/local/bin \
  && ln -s /usr/bin/python3 python \
  && pip3 install --upgrade pip

# ENV PYTHONPATH $PYTHONPATH:/app/hardware_report:/app/tests

# ENV PATH="$PATH:~/.local/bin"
RUN pip3 install tox setuptools wheel flake8

COPY . /app
RUN chown -R 10001:10001 /app

RUN pip3 install --upgrade pip && \
        pip3 install -r requirements.txt
