FROM ubuntu:latest

RUN apt-get update -qq && apt-get install -y python-dev python-pip wget default-jdk
RUN pip install --upgrade pip
RUN pip install numpy
RUN pip install pandas

RUN wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz
RUN tar -xf spark-1.6.1-bin-hadoop2.6.tgz
RUN rm spark-1.6.1-bin-hadoop2.6.tgz
RUN mv spark-* /usr/local/spark
ENV SPARK_HOME /usr/local/spark

ENV PATH $PATH:$SPARK_HOME/bin

RUN mkdir /recommendations
COPY elasticsearch-hadoop-2.3.2.jar .
COPY recommendations/ /recommendations/
