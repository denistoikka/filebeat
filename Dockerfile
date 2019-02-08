FROM debian:wheezy
MAINTAINER Denis Toikka <denis.toikka@gmail.com>

RUN apt-get update && apt-get install -y vim nano && apt-get clean

VOLUME /metadata
VOLUME /config
VOLUME /logs
WORKDIR /data
ADD build .

ENTRYPOINT ["/data/logging", "-config=/config/logging.json"]
