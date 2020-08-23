# oereb2-gretljobs
Contains GRETL jobs for publishing data to OEREB-Kataster

## Running a GRETL job locally using the GRETL wrapper script

For running a GRETL job using the GRETL wrapper script, see the `start-gretl.sh` command example below. Set any DB connection parameter environment variables before running the command.

If you want to set up development databases for developing new GRETL jobs, use the following `docker-compose` command before running your GRETL job in order to prepare the necessary DBs.

Start two DBs ("oereb" and "edit"),
import data required for the data transformation
(the so called legal basis data) into the "oereb" DB,
and import demo data into the "edit" DB
(when working on other OEREB topics, replace
`createSchemaLandUsePlans replaceDataLandUsePlans`
with the Gradle task names that handle your current OEREB topic):
```
docker-compose down # (this command is optional; it's just for cleaning up any already existing DB containers)
docker-compose run --rm --user $UID -v $PWD/development_dbs:/home/gradle/project gretl "sleep 20 && cd /home/gradle && gretl -b project/build-dev.gradle importFederalLegalBasisToOereb importFederalThemesToOereb importFederalTextToOereb  importCantonalResponsibleOfficeToOereb importCantonalLegalBasisToOereb importCantonalThemesToOereb createSchemaLandUsePlans replaceDataLandUsePlans"
```

Set environment variables containing the DB connection parameters
and names of other resources:
```
export ORG_GRADLE_PROJECT_dbUriEdit="jdbc:postgresql://edit-db/edit"
export ORG_GRADLE_PROJECT_dbUserEdit="gretl"
export ORG_GRADLE_PROJECT_dbPwdEdit="gretl"
export ORG_GRADLE_PROJECT_dbUriOereb="jdbc:postgresql://oereb-db/oereb"
export ORG_GRADLE_PROJECT_dbUserOereb="gretl"
export ORG_GRADLE_PROJECT_dbPwdOereb="gretl"
export ORG_GRADLE_PROJECT_geoservicesHostName="geo.so.ch"
```

Start the GRETL job
(use the --job-directory option to point to the desired GRETL job;
find out the names of your Docker networks by running `docker network ls`):
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_nutzungsplanung/ ....
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_nutzungsplanung/ deleteFromOereb
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_nutzungsplanung/ transferData


./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_bundesressourcen/ tasks --all
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_plzo/ tasks --all
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb2-gretljobs_default --job-directory $PWD/oereb_av/ tasks --all
```
