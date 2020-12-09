-- Create metax test user and test db

CREATE USER metax_test;
ALTER USER metax_test CREATEDB;
CREATE DATABASE metax_db_test OWNER metax_test ENCODING 'UTF8';