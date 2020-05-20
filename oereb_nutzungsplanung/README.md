## Developing

Im Ordner `dev/` ist sind in einer speziellen `build-dev.gradle`-Datei für die Entwicklung des GRETL-Jobs notwendigen Zusatz-Tasks, z.B. zum Befüllen der Edit-DB. Das produktive `build.gradle` soll *nicht* mit solchen Tasks ergänzt werden.

```
docker-compose up
```

Env-Variablen passend zur Entwicklungsumgebung setzen:
```
export ORG_GRADLE_PROJECT_dbUriEdit="jdbc:postgresql://edit-db/edit"
export ORG_GRADLE_PROJECT_dbUserEdit="gretl"
export ORG_GRADLE_PROJECT_dbPwdEdit="gretl"
export ORG_GRADLE_PROJECT_dbUriOereb="jdbc:postgresql://oereb-db/oereb"
export ORG_GRADLE_PROJECT_dbUserOereb="gretl"
export ORG_GRADLE_PROJECT_dbPwdOereb="gretl"
```

Leeres Nutzungsplanungschema in der Edit-DB erstellen:
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb-gretljobs_oerebgretljobs --job-directory $(pwd)/oereb_nutzungsplanung/dev/ -b build-dev.gradle createSchemaLandUsePlans
```

Nutzungsplanungsdaten in die Edit-DB importieren:
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb-gretljobs_oerebgretljobs --job-directory $(pwd)/oereb_nutzungsplanung/dev/ -b build-dev.gradle replaceLandUsePlansData
```

Bundesgesetze und kantonale Gesetze in die ÖREB-DB importieren:
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb-gretljobs_oerebgretljobs --job-directory $(pwd)/oereb_nutzungsplanung/dev/ -b build-dev.gradle importFederalLegalBasisToOereb importCantonalLegalBasisToOereb
```

Daten umbauen und exportieren:
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb-gretljobs_oerebgretljobs --job-directory $(pwd)/oereb_nutzungsplanung/ -b build.gradle validateData
```

Daten umbauen und in der ÖREB-DB in das Stage-Schema importieren:
```
./start-gretl.sh --docker-image sogis/gretl-runtime:latest --docker-network oereb-gretljobs_oerebgretljobs --job-directory $(pwd)/oereb_nutzungsplanung/ -b build.gradle importDataToStage

```
