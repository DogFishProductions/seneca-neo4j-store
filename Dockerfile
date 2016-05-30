# Version: 0.0.1

FROM node:4

MAINTAINER Paul Nebel "paul.nebel@redjamjar.net"
ENV REFRESHED_AT 2016_05_13
LABEL name="Base image for microservice testing"
LABEL version="1.0"

# Get up to date, install the bare necessities
# Create "redjam" user
# DANGEROUS: this is a dev convenience container, everyone has sudo access
RUN DEBIAN_FRONTEND=noninteractive sh -c '( \
    apt-get update -q && \
    apt-get install -y -q apt-utils curl wget vim man-db ssh bash-completion sudo xdg-utils build-essential && \
    apt-get clean && apt-get autoclean)' > /dev/null && \
    useradd -ms /bin/bash redjam && \
    chown -R redjam /usr/local && \
    chown -R redjam:redjam /home/redjam && \
    echo "ALL	ALL = (ALL) NOPASSWD: ALL" >> /etc/sudoers

# Set up some semblance of an environment
WORKDIR /home/redjam
ENV HOME /home/redjam
USER redjam

RUN npm install -g npm \
	&& npm install -g nodemon \
	&& npm config set python /usr/bin/python \
    && npm cache clear

WORKDIR /home/redjam
# Clone the repo locally
RUN git clone https://github.com/redjamjar/seneca-neo4j-store.git
RUN mv /home/redjam/seneca-neo4j-store /home/redjam/src
RUN mkdir /home/redjam/src/coverage
RUN chown -R redjam:redjam /home/redjam

WORKDIR /home/redjam/src
VOLUME /home/redjam/src
VOLUME /home/redjam/src/coverage