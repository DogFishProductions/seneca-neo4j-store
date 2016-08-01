# Version: 0.0.2

FROM risingstack/alpine:3.4-v4.4.4-3.6.1

MAINTAINER Paul Nebel "paul@nebel.io"
ENV REFRESHED_AT 2016_07_23
LABEL name="Base image for microservice testing"
LABEL version="1.0"

# Update the image and add required packages to run curl
RUN apk --update upgrade \
    && apk add curl ca-certificates \
    && mkdir /etc/ssl \
    && mkdir /etc/ssl/certs \
    && update-ca-certificates

# Create "dogfish" user
RUN addgroup appuser
RUN adduser -G appuser -g "App User" -h /home/dogfish -s /bin/ash -D dogfish \
    && chown -R dogfish:appuser /usr/local

# Set up some semblance of an environment
WORKDIR /home/dogfish
ENV HOME /home/dogfish
# Clone the repo locally
RUN git clone https://github.com/DogFishProductions/seneca-neo4j-store.git
RUN mv /home/dogfish/seneca-neo4j-store /home/dogfish/src
RUN mkdir /home/dogfish/src/coverage
RUN chown -R dogfish:appuser /home/dogfish

RUN npm install -g npm \
    && npm install -g nodemon \
    && npm config set python /usr/bin/python \
    && npm cache clear

USER dogfish

WORKDIR /home/dogfish/src
VOLUME /home/dogfish/src
VOLUME /home/dogfish/src/coverage