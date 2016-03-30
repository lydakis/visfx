FROM postgres:9.4

ENV POSTGRES_USER visfx
ENV POSTGRES_PASSWORD 012345

COPY data/ /tmp/data
RUN chmod -R 755 /tmp/data

COPY docker-entrypoint-initdb.d/init-user-db.sh docker-entrypoint-initdb.d/
RUN chmod 755 docker-entrypoint-initdb.d/init-user-db.sh
