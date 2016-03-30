FROM node:latest
RUN apt-get update -qq && apt-get install -y npm
RUN mkdir /visfx
WORKDIR /visfx

COPY config /visfx/config
COPY karma.conf.js /visfx/karma.conf.js
COPY package.json /visfx/package.json
COPY protractor.conf.js /visfx/protractor.conf.js
COPY tsconfig.json /visfx/tsconfig.json
COPY tslint.json /visfx/tslint.json
COPY typedoc.json /visfx/typedoc.json
COPY typings.json /visfx/typings.json
COPY webpack.config.js /visfx/webpack.config.js
RUN npm install

COPY src /visfx/src
