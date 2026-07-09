-- =============================================================
-- INIT.SQL — roda automaticamente na primeira criação do banco
-- =============================================================

-- Banco separado para o Airflow
CREATE DATABASE airflow_db;

-- Schemas da arquitetura medallion
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Permissões nos schemas
GRANT ALL PRIVILEGES ON SCHEMA bronze TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA silver TO pipeline_user;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO pipeline_user;

-- Permissões em tabelas futuras criadas pelo dbt ou pelos scripts
-- Sem isso, tabelas criadas por outros processos podem ficar inacessíveis
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO pipeline_user;

-- Confirma criação
DO $$
BEGIN
  RAISE NOTICE 'Schemas bronze, silver e gold criados com sucesso.';
END
$$;