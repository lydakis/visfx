FROM node:latest
RUN apt-get update -qq && apt-get install -y npm
RUN mkdir /visfx
WORKDIR /visfx

COPY . /visfx
RUN npm install
