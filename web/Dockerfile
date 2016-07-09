FROM node:latest
RUN apt-get update -qq && apt-get install -y npm python-dev python-pip
RUN pip install --upgrade pip
RUN pip install Flask flask-cors elasticsearch
RUN mkdir /visfx
WORKDIR /visfx
COPY . /visfx
RUN npm install
