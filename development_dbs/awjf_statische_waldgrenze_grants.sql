-- Grant privileges on schemas
GRANT USAGE
ON SCHEMA awjf_statische_waldgrenze, awjf_statische_waldgrenzen_oereb
TO public, gretl;

-- Grant read privileges
GRANT SELECT
ON ALL TABLES IN SCHEMA awjf_statische_waldgrenze, awjf_statische_waldgrenzen_oereb
TO public;

-- Grant write privileges
GRANT SELECT, INSERT, UPDATE, DELETE
ON ALL TABLES IN SCHEMA awjf_statische_waldgrenze, awjf_statische_waldgrenzen_oereb
TO gretl;
GRANT USAGE
ON ALL SEQUENCES IN SCHEMA awjf_statische_waldgrenze, awjf_statische_waldgrenzen_oereb
TO gretl;
