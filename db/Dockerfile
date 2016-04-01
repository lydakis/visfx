FROM postgres:9.4

ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD 012345

COPY docker-entrypoint-initdb.d/ docker-entrypoint-initdb.d/
RUN chmod 755 docker-entrypoint-initdb.d/
