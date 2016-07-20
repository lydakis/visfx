FROM node:latest
RUN apt-get update -qq && apt-get install -y npm python-dev python-pip
RUN mkdir /visfx
WORKDIR /visfx
COPY . /visfx
RUN npm install
