#!/bin/bash

rm -f /pgconf/setup.sql /pgconf/grants.sql
curl https://raw.githubusercontent.com/oereb/oereb2-db/master/sql/setup_original.sql > /pgconf/setup_original.sql
curl https://raw.githubusercontent.com/oereb/oereb2-db/master/sql/set_role.sql > /pgconf/set_role.sql
curl https://raw.githubusercontent.com/oereb/oereb2-db/master/sql/transfer_arp_npl_oereb_gdi.sql > /pgconf/transfer_arp_npl_oereb_gdi.sql
curl https://raw.githubusercontent.com/oereb/oereb2-db/master/sql/transfer_afu_grundwasserschutz_oereb_gdi.sql > /pgconf/transfer_afu_grundwasserschutz_oereb_gdi.sql
curl https://raw.githubusercontent.com/oereb/oereb2-db/master/sql/transfer_awjf_statische_waldgrenzen_oereb_gdi.sql > /pgconf/transfer_awjf_statische_waldgrenzen_oereb_gdi.sql
cat /pgconf/setup_original.sql /pgconf/set_role.sql /pgconf/transfer_*_oereb_gdi.sql > /pgconf/setup.sql
# Hack/unnecessary but required command (PG_WRITE_USER does not exist yet):
sed -i s/:PG_WRITE_USER/${PG_USER}/g /pgconf/setup.sql

echo "CREATE USER :PG_WRITE_USER WITH PASSWORD :'PG_WRITE_PASSWORD'; GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA arp_npl_oereb TO :PG_WRITE_USER; GRANT USAGE ON ALL SEQUENCES IN SCHEMA arp_npl_oereb TO :PG_WRITE_USER;" > /pgconf/grants.sql

exec "$@"
