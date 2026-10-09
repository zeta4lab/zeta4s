SELECT 'CREATE DATABASE prefect'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'prefect')\gexec

SELECT 'CREATE DATABASE zeta4s_metastore'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'zeta4s_metastore')\gexec

SELECT 'CREATE DATABASE lakekeeper'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'lakekeeper')\gexec
