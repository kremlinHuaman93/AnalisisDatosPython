FROM mysql:latest
WORKDIR /mysql/data
COPY data_warehouse_netflix.sql /docker-entrypoint-initdb.d/
COPY db_movies_neflix_transact.sql /docker-entrypoint-initdb.d/
COPY retail_db.sql /docker-entrypoint-initdb.d/