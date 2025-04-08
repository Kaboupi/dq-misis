-- INIT SCRIPT
CREATE SCHEMA IF NOT EXISTS data_quality_checks;

CREATE ROLE ro_dqp;

GRANT CONNECT ON DATABASE dqp_db TO ro_dqp;

GRANT USAGE ON SCHEMA data_quality_checks TO ro_dqp;

GRANT SELECT ON ALL TABLES IN SCHEMA data_quality_checks TO ro_dqp;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_quality_checks GRANT SELECT ON TABLES TO ro_dqp;

CREATE USER grafana_user WITH PASSWORD 'grafana_password';

GRANT ro_dqp TO grafana_user;