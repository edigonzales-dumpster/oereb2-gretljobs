CREATE SCHEMA IF NOT EXISTS afu_gewaesserschutz;
CREATE SEQUENCE afu_gewaesserschutz.t_ili2db_seq;;
-- Localisation_V1.LocalisedText
CREATE TABLE afu_gewaesserschutz.localisedtext (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,T_Type varchar(60) NOT NULL
  ,T_Seq bigint NULL
  ,alanguage varchar(255) NULL
  ,atext text NOT NULL
  ,multilingualtext_localisedtext bigint NULL
)
;
CREATE INDEX localisedtext_multilingualtext_lclsdtext_idx ON afu_gewaesserschutz.localisedtext ( multilingualtext_localisedtext );
-- Localisation_V1.LocalisedMText
CREATE TABLE afu_gewaesserschutz.localisedmtext (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,T_Type varchar(60) NOT NULL
  ,T_Seq bigint NULL
  ,alanguage varchar(255) NULL
  ,atext text NOT NULL
)
;
-- Localisation_V1.MultilingualText
CREATE TABLE afu_gewaesserschutz.multilingualtext (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,T_Type varchar(60) NOT NULL
  ,T_Seq bigint NULL
)
;
-- PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich
CREATE TABLE afu_gewaesserschutz.gsbereiche_gsbereich (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,identifikator varchar(255) NULL
  ,geometrie geometry(POLYGON,2056) NOT NULL
  ,typ varchar(255) NOT NULL
  ,kantonaletypbezeichnung text NULL
  ,kantonaletypbezeichnung_lang varchar(2) NULL
  ,bemerkungen text NULL
  ,bemerkungen_lang varchar(2) NULL
)
;
CREATE INDEX gsbereiche_gsbereich_geometrie_idx ON afu_gewaesserschutz.gsbereiche_gsbereich USING GIST ( geometrie );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Kanton_
CREATE TABLE afu_gewaesserschutz.gwszonen_kanton_ (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,T_Seq bigint NULL
  ,avalue varchar(255) NULL
  ,gwszonen_dokument_weiterekantone bigint NULL
)
;
CREATE INDEX gwszonen_kanton__gwszonen_dokumnt_wtrkntone_idx ON afu_gewaesserschutz.gwszonen_kanton_ ( gwszonen_dokument_weiterekantone );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status
CREATE TABLE afu_gewaesserschutz.gwszonen_status (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,rechtsstatus varchar(255) NOT NULL
  ,rechtskraftdatum date NULL
  ,bemerkungen text NULL
  ,bemerkungen_lang varchar(2) NULL
  ,kantonalerstatus text NULL
  ,kantonalerstatus_lang varchar(2) NULL
)
;
COMMENT ON TABLE afu_gewaesserschutz.gwszonen_status IS 'Rechtskraftdatum ist MANDATORY falls Rechtsstatus = "inKraft"';
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal
CREATE TABLE afu_gewaesserschutz.gwszonen_gwsareal (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,identifikator varchar(255) NULL
  ,geometrie geometry(POLYGON,2056) NOT NULL
  ,bemerkungen text NULL
  ,bemerkungen_lang varchar(2) NULL
  ,typ varchar(255) NOT NULL
  ,istaltrechtlich boolean NOT NULL
  ,astatus bigint NOT NULL
)
;
CREATE INDEX gwszonen_gwsareal_geometrie_idx ON afu_gewaesserschutz.gwszonen_gwsareal USING GIST ( geometrie );
CREATE INDEX gwszonen_gwsareal_astatus_idx ON afu_gewaesserschutz.gwszonen_gwsareal ( astatus );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone
CREATE TABLE afu_gewaesserschutz.gwszonen_gwszone (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,identifikator varchar(255) NULL
  ,geometrie geometry(POLYGON,2056) NOT NULL
  ,bemerkungen text NULL
  ,bemerkungen_lang varchar(2) NULL
  ,typ varchar(255) NOT NULL
  ,kantonaletypbezeichnung text NULL
  ,kantonaletypbezeichnung_lang varchar(2) NULL
  ,istaltrechtlich boolean NOT NULL
  ,astatus bigint NOT NULL
)
;
CREATE INDEX gwszonen_gwszone_geometrie_idx ON afu_gewaesserschutz.gwszonen_gwszone USING GIST ( geometrie );
CREATE INDEX gwszonen_gwszone_astatus_idx ON afu_gewaesserschutz.gwszonen_gwszone ( astatus );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument
CREATE TABLE afu_gewaesserschutz.gwszonen_dokument (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,art varchar(255) NOT NULL
  ,titel varchar(80) NOT NULL
  ,offiziellertitel text NULL
  ,abkuerzung varchar(10) NULL
  ,offiziellenr varchar(20) NULL
  ,kanton varchar(255) NULL
  ,gemeinde integer NULL
  ,publiziertab date NOT NULL
  ,rechtsstatus varchar(255) NOT NULL
  ,textimweb varchar(1023) NULL
  ,dokument bytea NULL
)
;
COMMENT ON COLUMN afu_gewaesserschutz.gwszonen_dokument.dokument IS 'Das Dokument als PDF-Datei';
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente
CREATE TABLE afu_gewaesserschutz.gwszonen_hinweisweiteredokumente (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,ursprung bigint NOT NULL
  ,hinweis bigint NOT NULL
)
;
CREATE INDEX gwszonen_hinweiswetrdkmnte_ursprung_idx ON afu_gewaesserschutz.gwszonen_hinweisweiteredokumente ( ursprung );
CREATE INDEX gwszonen_hinweiswetrdkmnte_hinweis_idx ON afu_gewaesserschutz.gwszonen_hinweisweiteredokumente ( hinweis );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal
CREATE TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwsareal (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,rechtsvorschrift bigint NOT NULL
  ,gwsareal bigint NOT NULL
)
;
CREATE INDEX gwsznn_rchtsvschrftgwsreal_rechtsvorschrift_idx ON afu_gewaesserschutz.gwszonen_rechtsvorschriftgwsareal ( rechtsvorschrift );
CREATE INDEX gwsznn_rchtsvschrftgwsreal_gwsareal_idx ON afu_gewaesserschutz.gwszonen_rechtsvorschriftgwsareal ( gwsareal );
-- PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone
CREATE TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwszone (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,rechtsvorschrift bigint NOT NULL
  ,gwszone bigint NOT NULL
)
;
CREATE INDEX gwsznn_rchtsvschrftgwszone_rechtsvorschrift_idx ON afu_gewaesserschutz.gwszonen_rechtsvorschriftgwszone ( rechtsvorschrift );
CREATE INDEX gwsznn_rchtsvschrftgwszone_gwszone_idx ON afu_gewaesserschutz.gwszonen_rechtsvorschriftgwszone ( gwszone );
-- PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt
CREATE TABLE afu_gewaesserschutz.transfermetadaten_amt (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,aname text NULL
  ,aname_de text NULL
  ,aname_fr text NULL
  ,aname_rm text NULL
  ,aname_it text NULL
  ,aname_en text NULL
  ,amtimweb varchar(1023) NULL
  ,auid varchar(12) NULL
)
;
COMMENT ON TABLE afu_gewaesserschutz.transfermetadaten_amt IS 'Eine organisatorische Einheit innerhalb der öffentlichen Verwaltung, z.B. eine für Geobasisdaten zuständigen Stelle.';
COMMENT ON COLUMN afu_gewaesserschutz.transfermetadaten_amt.amtimweb IS 'Verweis auf die Website des Amtes z.B. "http://www.jgk.be.ch/site/agr/".';
-- PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst
CREATE TABLE afu_gewaesserschutz.transfermetadaten_darstellungsdienst (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,verweiswms varchar(1023) NULL
  ,legendeimweb varchar(1023) NULL
)
;
COMMENT ON TABLE afu_gewaesserschutz.transfermetadaten_darstellungsdienst IS 'Angaben zum Darstellungsdienst.';
COMMENT ON COLUMN afu_gewaesserschutz.transfermetadaten_darstellungsdienst.verweiswms IS 'WMS GetMap-Request (für Maschine-Maschine-Kommunikation) inkl. alle benötigten Parameter, z.B. "http://ecogis.admin.ch/de/wms?version=1.1.1&service=WMS&request=GetMap&LAYERS=invent_leit&srs=EPSG:21781&WIDTH=500&HEIGHT=500&bbox=500000,100000,700000,300000&FORMAT=image/png&TRANSPARENT=TRUE"';
COMMENT ON COLUMN afu_gewaesserschutz.transfermetadaten_darstellungsdienst.legendeimweb IS 'Verweis auf ein Dokument das die Karte beschreibt; z.B. "http://www.apps.be.ch/geoportal/output/gdp_adm_x3012app0081524772611.png"';
-- PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand
CREATE TABLE afu_gewaesserschutz.transfermetadaten_datenbestand (
  T_Id bigint PRIMARY KEY DEFAULT nextval('afu_gewaesserschutz.t_ili2db_seq')
  ,basketid text NOT NULL
  ,stand date NOT NULL
  ,lieferdatum date NULL
  ,bemerkungen text NULL
  ,weiteremetadaten varchar(1023) NULL
  ,darstellungsdienst bigint NOT NULL
  ,zustaendigestelle bigint NOT NULL
)
;
CREATE INDEX transfermetadatn_dtnbstand_darstellungsdienst_idx ON afu_gewaesserschutz.transfermetadaten_datenbestand ( darstellungsdienst );
CREATE INDEX transfermetadatn_dtnbstand_zustaendigestelle_idx ON afu_gewaesserschutz.transfermetadaten_datenbestand ( zustaendigestelle );
COMMENT ON COLUMN afu_gewaesserschutz.transfermetadaten_datenbestand.weiteremetadaten IS 'Verweis auf weitere maschinenlesbare Metadaten (XML). z.B. http://www.geocat.ch/geonetwork/srv/deu/gm03.xml?id=705';
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_BASKET (
  T_Id bigint PRIMARY KEY
  ,dataset bigint NULL
  ,topic varchar(200) NOT NULL
  ,T_Ili_Tid varchar(200) NULL
  ,attachmentKey varchar(200) NOT NULL
  ,domains varchar(1024) NULL
)
;
CREATE INDEX T_ILI2DB_BASKET_dataset_idx ON afu_gewaesserschutz.t_ili2db_basket ( dataset );
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_DATASET (
  T_Id bigint PRIMARY KEY
  ,datasetName varchar(200) NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_INHERITANCE (
  thisClass varchar(1024) PRIMARY KEY
  ,baseClass varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_SETTINGS (
  tag varchar(60) PRIMARY KEY
  ,setting varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_TRAFO (
  iliname varchar(1024) NOT NULL
  ,tag varchar(1024) NOT NULL
  ,setting varchar(1024) NOT NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_MODEL (
  filename varchar(250) NOT NULL
  ,iliversion varchar(3) NOT NULL
  ,modelName text NOT NULL
  ,content text NOT NULL
  ,importDate timestamp NOT NULL
  ,PRIMARY KEY (iliversion,modelName)
)
;
CREATE TABLE afu_gewaesserschutz.chcantoncode (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.gwszonen_schutzzonetyp (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.gwszonen_schutzarealtyp (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.gwszonen_dokumentart (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.rechtsstatusart (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.languagecode_iso639_1 (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.gsbereiche_gsbereichtyp (
  itfCode integer PRIMARY KEY
  ,iliCode varchar(1024) NOT NULL
  ,seq integer NULL
  ,inactive boolean NOT NULL
  ,dispName varchar(250) NOT NULL
  ,description varchar(1024) NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_CLASSNAME (
  IliName varchar(1024) PRIMARY KEY
  ,SqlName varchar(1024) NOT NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_ATTRNAME (
  IliName varchar(1024) NOT NULL
  ,SqlName varchar(1024) NOT NULL
  ,ColOwner varchar(1024) NOT NULL
  ,Target varchar(1024) NULL
  ,PRIMARY KEY (ColOwner,SqlName)
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (
  tablename varchar(255) NOT NULL
  ,subtype varchar(255) NULL
  ,columnname varchar(255) NOT NULL
  ,tag varchar(1024) NOT NULL
  ,setting varchar(1024) NOT NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (
  tablename varchar(255) NOT NULL
  ,tag varchar(1024) NOT NULL
  ,setting varchar(1024) NOT NULL
)
;
CREATE TABLE afu_gewaesserschutz.T_ILI2DB_META_ATTRS (
  ilielement varchar(255) NOT NULL
  ,attr_name varchar(1024) NOT NULL
  ,attr_value varchar(1024) NOT NULL
)
;
ALTER TABLE afu_gewaesserschutz.localisedtext ADD CONSTRAINT localisedtext_multilingualtext_lclsdtext_fkey FOREIGN KEY ( multilingualtext_localisedtext ) REFERENCES afu_gewaesserschutz.multilingualtext DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_kanton_ ADD CONSTRAINT gwszonen_kanton__gwszonen_dokumnt_wtrkntone_fkey FOREIGN KEY ( gwszonen_dokument_weiterekantone ) REFERENCES afu_gewaesserschutz.gwszonen_dokument DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_gwsareal ADD CONSTRAINT gwszonen_gwsareal_astatus_fkey FOREIGN KEY ( astatus ) REFERENCES afu_gewaesserschutz.gwszonen_status DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_gwszone ADD CONSTRAINT gwszonen_gwszone_astatus_fkey FOREIGN KEY ( astatus ) REFERENCES afu_gewaesserschutz.gwszonen_status DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_dokument ADD CONSTRAINT gwszonen_dokument_gemeinde_check CHECK( gemeinde BETWEEN 1 AND 9999);
ALTER TABLE afu_gewaesserschutz.gwszonen_hinweisweiteredokumente ADD CONSTRAINT gwszonen_hinweiswetrdkmnte_ursprung_fkey FOREIGN KEY ( ursprung ) REFERENCES afu_gewaesserschutz.gwszonen_dokument DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_hinweisweiteredokumente ADD CONSTRAINT gwszonen_hinweiswetrdkmnte_hinweis_fkey FOREIGN KEY ( hinweis ) REFERENCES afu_gewaesserschutz.gwszonen_dokument DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwsareal ADD CONSTRAINT gwsznn_rchtsvschrftgwsreal_rechtsvorschrift_fkey FOREIGN KEY ( rechtsvorschrift ) REFERENCES afu_gewaesserschutz.gwszonen_dokument DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwsareal ADD CONSTRAINT gwsznn_rchtsvschrftgwsreal_gwsareal_fkey FOREIGN KEY ( gwsareal ) REFERENCES afu_gewaesserschutz.gwszonen_gwsareal DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwszone ADD CONSTRAINT gwsznn_rchtsvschrftgwszone_rechtsvorschrift_fkey FOREIGN KEY ( rechtsvorschrift ) REFERENCES afu_gewaesserschutz.gwszonen_dokument DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.gwszonen_rechtsvorschriftgwszone ADD CONSTRAINT gwsznn_rchtsvschrftgwszone_gwszone_fkey FOREIGN KEY ( gwszone ) REFERENCES afu_gewaesserschutz.gwszonen_gwszone DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.transfermetadaten_datenbestand ADD CONSTRAINT transfermetadatn_dtnbstand_darstellungsdienst_fkey FOREIGN KEY ( darstellungsdienst ) REFERENCES afu_gewaesserschutz.transfermetadaten_darstellungsdienst DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.transfermetadaten_datenbestand ADD CONSTRAINT transfermetadatn_dtnbstand_zustaendigestelle_fkey FOREIGN KEY ( zustaendigestelle ) REFERENCES afu_gewaesserschutz.transfermetadaten_amt DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE afu_gewaesserschutz.T_ILI2DB_BASKET ADD CONSTRAINT T_ILI2DB_BASKET_dataset_fkey FOREIGN KEY ( dataset ) REFERENCES afu_gewaesserschutz.T_ILI2DB_DATASET DEFERRABLE INITIALLY DEFERRED;
CREATE UNIQUE INDEX T_ILI2DB_DATASET_datasetName_key ON afu_gewaesserschutz.T_ILI2DB_DATASET (datasetName)
;
CREATE UNIQUE INDEX T_ILI2DB_MODEL_iliversion_modelName_key ON afu_gewaesserschutz.T_ILI2DB_MODEL (iliversion,modelName)
;
CREATE UNIQUE INDEX T_ILI2DB_ATTRNAME_ColOwner_SqlName_key ON afu_gewaesserschutz.T_ILI2DB_ATTRNAME (ColOwner,SqlName)
;
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSAreal','gwszonen_statusgwsareal');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone','gwszonen_gwszone');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.DarstellungsdienstDatenbestand','transfermetadaten_darstellungsdienstdatenbestand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status','gwszonen_status');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone','gwszonen_rechtsvorschriftgwszone');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.SchutzzoneTyp','gwszonen_schutzzonetyp');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('InternationalCodes_V1.LanguageCode_ISO639_1','languagecode_iso639_1');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal','gwszonen_gwsareal');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Kanton_','gwszonen_kanton_');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.zustaendigeStelleDatenbestand','transfermetadaten_zustaendigestelledatenbestand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('Localisation_V1.LocalisedText','localisedtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich','gsbereiche_gsbereich');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente','gwszonen_hinweisweiteredokumente');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand','transfermetadaten_datenbestand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereichTyp','gsbereiche_gsbereichtyp');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('LocalisationCH_V1.MultilingualText','localisationch_v1_multilingualtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal','gwszonen_rechtsvorschriftgwsareal');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('LocalisationCH_V1.LocalisedText','localisationch_v1_localisedtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSZone','gwszonen_statusgwszone');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.DokumentArt','gwszonen_dokumentart');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('LocalisationCH_V1.LocalisedMText','localisationch_v1_localisedmtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.SchutzarealTyp','gwszonen_schutzarealtyp');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.RechtsstatusArt','rechtsstatusart');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('Localisation_V1.LocalisedMText','localisedmtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst','transfermetadaten_darstellungsdienst');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('CHAdminCodes_V1.CHCantonCode','chcantoncode');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('Localisation_V1.MultilingualText','multilingualtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_CLASSNAME (IliName,SqlName) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt','transfermetadaten_amt');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.Geometrie','geometrie','gwszonen_gwsareal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Art','art','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.Typ','typ','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSAreal.Status','astatus','gwszonen_gwsareal','gwszonen_status');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente.Hinweis','hinweis','gwszonen_hinweisweiteredokumente','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSZone.Status','astatus','gwszonen_gwszone','gwszonen_status');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente.Ursprung','ursprung','gwszonen_hinweisweiteredokumente','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.KantonaleTypBezeichnung','kantonaletypbezeichnung','gsbereiche_gsbereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.DarstellungsdienstDatenbestand.Darstellungsdienst','darstellungsdienst','transfermetadaten_datenbestand','transfermetadaten_darstellungsdienst');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Kanton_.Value','avalue','gwszonen_kanton_',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.istAltrechtlich','istaltrechtlich','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.OffiziellerTitel','offiziellertitel','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.TextImWeb','textimweb','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.istAltrechtlich','istaltrechtlich','gwszonen_gwsareal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt.UID','auid','transfermetadaten_amt',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Titel','titel','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst.LegendeImWeb','legendeimweb','transfermetadaten_darstellungsdienst',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('Localisation_V1.LocalisedText.Language','alanguage','localisedtext',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.Typ','typ','gsbereiche_gsbereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand.Bemerkungen','bemerkungen','transfermetadaten_datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.Geometrie','geometrie','gsbereiche_gsbereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone.GWSZone','gwszone','gwszonen_rechtsvorschriftgwszone','gwszonen_gwszone');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.Bemerkungen','bemerkungen','gsbereiche_gsbereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.Bemerkungen','bemerkungen','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.WeitereKantone','gwszonen_dokument_weiterekantone','gwszonen_kanton_','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.Geometrie','geometrie','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.Typ','typ','gwszonen_gwsareal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand.BasketId','basketid','transfermetadaten_datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Dokument','dokument','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal.GWSAreal','gwsareal','gwszonen_rechtsvorschriftgwsareal','gwszonen_gwsareal');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.zustaendigeStelleDatenbestand.zustaendigeStelle','zustaendigestelle','transfermetadaten_datenbestand','transfermetadaten_amt');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Gemeinde','gemeinde','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('Localisation_V1.LocalisedMText.Language','alanguage','localisedmtext',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt.AmtImWeb','amtimweb','transfermetadaten_amt',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand.weitereMetadaten','weiteremetadaten','transfermetadaten_datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand.Stand','stand','transfermetadaten_datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.KantonaleTypBezeichnung','kantonaletypbezeichnung','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Kanton','kanton','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.Identifikator','identifikator','gwszonen_gwszone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('Localisation_V1.LocalisedMText.Text','atext','localisedmtext',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Rechtsstatus','rechtsstatus','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.Rechtsstatus','rechtsstatus','gwszonen_status',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.Rechtskraftdatum','rechtskraftdatum','gwszonen_status',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.KantonalerStatus','kantonalerstatus','gwszonen_status',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.Bemerkungen','bemerkungen','gwszonen_status',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst.VerweisWMS','verweiswms','transfermetadaten_darstellungsdienst',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone.Rechtsvorschrift','rechtsvorschrift','gwszonen_rechtsvorschriftgwszone','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal.Rechtsvorschrift','rechtsvorschrift','gwszonen_rechtsvorschriftgwsareal','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('Localisation_V1.LocalisedText.Text','atext','localisedtext',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.publiziertAb','publiziertab','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.Bemerkungen','bemerkungen','gwszonen_gwsareal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.OffizielleNr','offiziellenr','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt.Name','aname','transfermetadaten_amt',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument.Abkuerzung','abkuerzung','gwszonen_dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.Identifikator','identifikator','gwszonen_gwsareal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('Localisation_V1.MultilingualText.LocalisedText','multilingualtext_localisedtext','localisedtext','multilingualtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand.Lieferdatum','lieferdatum','transfermetadaten_datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_ATTRNAME (IliName,SqlName,ColOwner,Target) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.Identifikator','identifikator','gsbereiche_gsbereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSAreal','ch.ehi.ili2db.inheritance','embedded');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.DarstellungsdienstDatenbestand','ch.ehi.ili2db.inheritance','embedded');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.Bemerkungen','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.Bemerkungen','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Kanton_','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.zustaendigeStelleDatenbestand','ch.ehi.ili2db.inheritance','embedded');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('Localisation_V1.LocalisedText','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt.Name','ch.ehi.ili2db.multilingualTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('LocalisationCH_V1.MultilingualText','ch.ehi.ili2db.inheritance','superClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.KantonalerStatus','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('LocalisationCH_V1.LocalisedText','ch.ehi.ili2db.inheritance','superClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSZone','ch.ehi.ili2db.inheritance','embedded');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('LocalisationCH_V1.LocalisedMText','ch.ehi.ili2db.inheritance','superClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal.Bemerkungen','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status.Bemerkungen','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone.KantonaleTypBezeichnung','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('Localisation_V1.LocalisedMText','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('Localisation_V1.MultilingualText','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt','ch.ehi.ili2db.inheritance','newClass');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TRAFO (iliname,tag,setting) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich.KantonaleTypBezeichnung','ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.DarstellungsdienstDatenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSZone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Datenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Dokument',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSAreal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('Localisation_V1.LocalisedText',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Darstellungsdienst',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.HinweisWeitereDokumente',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.StatusGWSAreal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSAreal',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('LocalisationCH_V1.LocalisedText','Localisation_V1.LocalisedText');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('LocalisationCH_V1.MultilingualText','Localisation_V1.MultilingualText');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('Localisation_V1.MultilingualText',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.GWSZone',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Kanton_',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GSBereiche.GSBereich',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('LocalisationCH_V1.LocalisedMText','Localisation_V1.LocalisedMText');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.zustaendigeStelleDatenbestand',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('Localisation_V1.LocalisedMText',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.Status',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.TransferMetadaten.Amt',NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_INHERITANCE (thisClass,baseClass) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1.GWSZonen.RechtsvorschriftGWSZone',NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZH',0,'ZH',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'BE',1,'BE',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'LU',2,'LU',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'UR',3,'UR',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'SZ',4,'SZ',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'OW',5,'OW',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'NW',6,'NW',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'GL',7,'GL',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZG',8,'ZG',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'FR',9,'FR',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'SO',10,'SO',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'BS',11,'BS',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'BL',12,'BL',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'SH',13,'SH',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'AR',14,'AR',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'AI',15,'AI',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'SG',16,'SG',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'GR',17,'GR',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'AG',18,'AG',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'TG',19,'TG',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'TI',20,'TI',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'VD',21,'VD',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'VS',22,'VS',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'NE',23,'NE',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'GE',24,'GE',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'JU',25,'JU',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'FL',26,'FL',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.chcantoncode (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'CH',27,'CH',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'S1',0,'S1',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'S2',1,'S2',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'S3',2,'S3',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'S3Zu',3,'S3Zu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'S_kantonaleArt',4,'S kantonaleArt',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Sh',5,'Sh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzzonetyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Sm',6,'Sm',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Areal',0,'Areal',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZukuenftigeZoneS1',1,'ZukuenftigeZoneS1',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZukuenftigeZoneS2',2,'ZukuenftigeZoneS2',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZukuenftigeZoneS3',3,'ZukuenftigeZoneS3',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZukuenftigeZoneSh',4,'ZukuenftigeZoneSh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_schutzarealtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ZukuenftigeZoneSm',5,'ZukuenftigeZoneSm',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_dokumentart (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Rechtsvorschrift',0,'Rechtsvorschrift',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_dokumentart (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'GesetzlicheGrundlage',1,'GesetzlicheGrundlage',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gwszonen_dokumentart (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Hinweis',2,'Hinweis',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.rechtsstatusart (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'inKraft',0,'inKraft',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.rechtsstatusart (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'provisorisch',1,'provisorisch',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'de',0,'de',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fr',1,'fr',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'it',2,'it',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'rm',3,'rm',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'en',4,'en',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'aa',5,'aa',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ab',6,'ab',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'af',7,'af',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'am',8,'am',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ar',9,'ar',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'as',10,'as',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ay',11,'ay',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'az',12,'az',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ba',13,'ba',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'be',14,'be',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'bg',15,'bg',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'bh',16,'bh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'bi',17,'bi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'bn',18,'bn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'bo',19,'bo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'br',20,'br',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ca',21,'ca',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'co',22,'co',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'cs',23,'cs',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'cy',24,'cy',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'da',25,'da',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'dz',26,'dz',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'el',27,'el',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'eo',28,'eo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'es',29,'es',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'et',30,'et',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'eu',31,'eu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fa',32,'fa',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fi',33,'fi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fj',34,'fj',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fo',35,'fo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'fy',36,'fy',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ga',37,'ga',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'gd',38,'gd',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'gl',39,'gl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'gn',40,'gn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'gu',41,'gu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ha',42,'ha',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'he',43,'he',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'hi',44,'hi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'hr',45,'hr',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'hu',46,'hu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'hy',47,'hy',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ia',48,'ia',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'id',49,'id',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ie',50,'ie',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ik',51,'ik',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'is',52,'is',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'iu',53,'iu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ja',54,'ja',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'jw',55,'jw',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ka',56,'ka',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'kk',57,'kk',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'kl',58,'kl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'km',59,'km',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'kn',60,'kn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ko',61,'ko',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ks',62,'ks',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ku',63,'ku',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ky',64,'ky',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'la',65,'la',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ln',66,'ln',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'lo',67,'lo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'lt',68,'lt',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'lv',69,'lv',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mg',70,'mg',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mi',71,'mi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mk',72,'mk',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ml',73,'ml',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mn',74,'mn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mo',75,'mo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mr',76,'mr',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ms',77,'ms',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'mt',78,'mt',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'my',79,'my',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'na',80,'na',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ne',81,'ne',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'nl',82,'nl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'no',83,'no',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'oc',84,'oc',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'om',85,'om',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'or',86,'or',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'pa',87,'pa',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'pl',88,'pl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ps',89,'ps',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'pt',90,'pt',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'qu',91,'qu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'rn',92,'rn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ro',93,'ro',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ru',94,'ru',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'rw',95,'rw',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sa',96,'sa',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sd',97,'sd',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sg',98,'sg',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sh',99,'sh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'si',100,'si',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sk',101,'sk',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sl',102,'sl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sm',103,'sm',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sn',104,'sn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'so',105,'so',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sq',106,'sq',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sr',107,'sr',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ss',108,'ss',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'st',109,'st',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'su',110,'su',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sv',111,'sv',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'sw',112,'sw',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ta',113,'ta',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'te',114,'te',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tg',115,'tg',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'th',116,'th',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ti',117,'ti',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tk',118,'tk',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tl',119,'tl',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tn',120,'tn',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'to',121,'to',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tr',122,'tr',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ts',123,'ts',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tt',124,'tt',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'tw',125,'tw',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ug',126,'ug',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'uk',127,'uk',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'ur',128,'ur',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'uz',129,'uz',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'vi',130,'vi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'vo',131,'vo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'wo',132,'wo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'xh',133,'xh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'yi',134,'yi',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'yo',135,'yo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'za',136,'za',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'zh',137,'zh',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.languagecode_iso639_1 (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'zu',138,'zu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gsbereiche_gsbereichtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Ao',0,'Ao',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gsbereiche_gsbereichtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Au',1,'Au',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gsbereiche_gsbereichtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Zo',2,'Zo',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gsbereiche_gsbereichtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'Zu',3,'Zu',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.gsbereiche_gsbereichtyp (seq,iliCode,itfCode,dispName,inactive,description) VALUES (NULL,'UB',4,'UB',FALSE,NULL);
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('localisedtext',NULL,'T_Type','ch.ehi.ili2db.types','["localisationch_v1_localisedtext","localisedtext"]');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('multilingualtext',NULL,'T_Type','ch.ehi.ili2db.types','["localisationch_v1_multilingualtext","multilingualtext"]');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwszone',NULL,'gwszone','ch.ehi.ili2db.foreignKey','gwszonen_gwszone');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.c1Max','2870000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.c1Min','2460000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.coordDimension','2');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.c2Max','1310000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.geomType','POLYGON');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.c2Min','1045000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gsbereiche_gsbereich',NULL,'geometrie','ch.ehi.ili2db.srid','2056');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('transfermetadaten_datenbestand',NULL,'zustaendigestelle','ch.ehi.ili2db.foreignKey','transfermetadaten_amt');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwszone',NULL,'rechtsvorschrift','ch.ehi.ili2db.foreignKey','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.c1Max','2870000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.c1Min','2460000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.coordDimension','2');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.c2Max','1310000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.geomType','POLYGON');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.c2Min','1045000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'geometrie','ch.ehi.ili2db.srid','2056');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwsareal',NULL,'gwsareal','ch.ehi.ili2db.foreignKey','gwszonen_gwsareal');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwsareal',NULL,'astatus','ch.ehi.ili2db.foreignKey','gwszonen_status');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('localisedtext',NULL,'multilingualtext_localisedtext','ch.ehi.ili2db.foreignKey','multilingualtext');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('localisedmtext',NULL,'T_Type','ch.ehi.ili2db.types','["localisationch_v1_localisedmtext","localisedmtext"]');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_hinweisweiteredokumente',NULL,'hinweis','ch.ehi.ili2db.foreignKey','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_hinweisweiteredokumente',NULL,'ursprung','ch.ehi.ili2db.foreignKey','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('localisedmtext',NULL,'atext','ch.ehi.ili2db.textKind','MTEXT');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'astatus','ch.ehi.ili2db.foreignKey','gwszonen_status');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_kanton_',NULL,'gwszonen_dokument_weiterekantone','ch.ehi.ili2db.foreignKey','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.c1Max','2870000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.c1Min','2460000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.coordDimension','2');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.c2Max','1310000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.geomType','POLYGON');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.c2Min','1045000.000');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_gwszone',NULL,'geometrie','ch.ehi.ili2db.srid','2056');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('transfermetadaten_datenbestand',NULL,'bemerkungen','ch.ehi.ili2db.textKind','MTEXT');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwsareal',NULL,'rechtsvorschrift','ch.ehi.ili2db.foreignKey','gwszonen_dokument');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_COLUMN_PROP (tablename,subtype,columnname,tag,setting) VALUES ('transfermetadaten_datenbestand',NULL,'darstellungsdienst','ch.ehi.ili2db.foreignKey','transfermetadaten_darstellungsdienst');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_schutzarealtyp','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('languagecode_iso639_1','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gsbereiche_gsbereichtyp','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_schutzzonetyp','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('localisedmtext','ch.ehi.ili2db.tableKind','STRUCTURE');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('multilingualtext','ch.ehi.ili2db.tableKind','STRUCTURE');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_dokumentart','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_kanton_','ch.ehi.ili2db.tableKind','STRUCTURE');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('transfermetadaten_amt','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwsareal','ch.ehi.ili2db.tableKind','ASSOCIATION');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_hinweisweiteredokumente','ch.ehi.ili2db.tableKind','ASSOCIATION');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_rechtsvorschriftgwszone','ch.ehi.ili2db.tableKind','ASSOCIATION');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_status','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_dokument','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('rechtsstatusart','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gsbereiche_gsbereich','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('transfermetadaten_darstellungsdienst','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_gwszone','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('localisedtext','ch.ehi.ili2db.tableKind','STRUCTURE');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('gwszonen_gwsareal','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('chcantoncode','ch.ehi.ili2db.tableKind','ENUM');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_TABLE_PROP (tablename,tag,setting) VALUES ('transfermetadaten_datenbestand','ch.ehi.ili2db.tableKind','CLASS');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('PlanerischerGewaesserschutz_V1_1.ili','2.3','PlanerischerGewaesserschutz_LV03_V1_1{ LocalisationCH_V1 Units CHAdminCodes_V1 GeometryCHLV03_V1} PlanerischerGewaesserschutz_LV95_V1_1{ LocalisationCH_V1 Units CHAdminCodes_V1 GeometryCHLV95_V1}','INTERLIS 2.3;
  
!!@ technicalContact=mailto:gis@bafu.admin.ch
!!@ furtherInformation=https://www.bafu.admin.ch/geodatenmodelle
!!@ IDGeoIV="130.1,131.1,132.1"
MODEL PlanerischerGewaesserschutz_LV03_V1_1 (de)
AT "https://models.geo.admin.ch/BAFU/"
VERSION "2017-06-20"  =
  IMPORTS LocalisationCH_V1,CHAdminCodes_V1,Units,GeometryCHLV03_V1;

  UNIT

    CubicMeterPerSecond [m3sec] = (Units.m3 / INTERLIS.s);

  DOMAIN

    Menge = 0 .. 100000 [m3sec];

    RechtsstatusArt = (
      inKraft,
      provisorisch
    );

    CHSurface = SURFACE WITH (STRAIGHTS) VERTEX GeometryCHLV03_V1.Coord2 WITHOUT OVERLAPS > 0.001;

  TOPIC GSBereiche =

    DOMAIN

      GSBereichTyp = (
        Ao,
        Au,
        Zo,
        Zu,
        UB
      );

    CLASS GSBereich =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV03_V1_1.CHSurface;
      Typ : MANDATORY GSBereichTyp;
      KantonaleTypBezeichnung : LocalisationCH_V1.LocalisedText;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
    END GSBereich;

  END GSBereiche;

  TOPIC GWSZonen =

    DOMAIN

      SchutzarealTyp = (
        Areal,
        ZukuenftigeZoneS1,
        ZukuenftigeZoneS2,
        ZukuenftigeZoneS3,
        ZukuenftigeZoneSh,
        ZukuenftigeZoneSm
      );

      SchutzzoneTyp = (
        S1,
        S2,
        S3,
        S3Zu,
        S_kantonaleArt,
        Sh,
        Sm
      );

      DokumentArt = (
        Rechtsvorschrift,
        GesetzlicheGrundlage,
        Hinweis
      );

    STRUCTURE Kanton_ = 
      Value: CHAdminCodes_V1.CHCantonCode;
    END Kanton_;

    /** Rechtskraftdatum ist MANDATORY falls Rechtsstatus = "inKraft"
     */
    CLASS Status =
      Rechtsstatus : MANDATORY PlanerischerGewaesserschutz_LV03_V1_1.RechtsstatusArt;
      Rechtskraftdatum : INTERLIS.XMLDate;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      KantonalerStatus : LocalisationCH_V1.LocalisedText;
      MANDATORY CONSTRAINT Rechtsstatus!=#inKraft OR DEFINED(Rechtskraftdatum);
    END Status;

    CLASS GWSAreal =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV03_V1_1.CHSurface;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      Typ : MANDATORY SchutzarealTyp;
      istAltrechtlich : MANDATORY BOOLEAN;
    END GWSAreal;

    CLASS GWSZone =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV03_V1_1.CHSurface;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      Typ : MANDATORY SchutzzoneTyp;
      KantonaleTypBezeichnung : LocalisationCH_V1.LocalisedText;
      istAltrechtlich : MANDATORY BOOLEAN;
    END GWSZone;

    ASSOCIATION StatusGWSAreal =
      Status -- {1} Status;
      GWSAreal -<> {0..*} GWSAreal;
    END StatusGWSAreal;

    ASSOCIATION StatusGWSZone =
      Status -- {1} Status;
      GWSZone -<> {0..*} GWSZone;
    END StatusGWSZone;

    CLASS Dokument =
      Art : MANDATORY DokumentArt;
      Titel : MANDATORY TEXT*80;
      OffiziellerTitel : TEXT;
      Abkuerzung : TEXT*10;
      OffizielleNr : TEXT*20;
      Kanton : CHAdminCodes_V1.CHCantonCode;
      WeitereKantone : BAG {0..*} OF Kanton_;
      Gemeinde : CHAdminCodes_V1.CHMunicipalityCode;
      publiziertAb : MANDATORY INTERLIS.XMLDate;
      Rechtsstatus : MANDATORY PlanerischerGewaesserschutz_LV03_V1_1.RechtsstatusArt;
      TextImWeb : URI;
      /** Das Dokument als PDF-Datei
       */
      Dokument : BLACKBOX BINARY;
      MANDATORY CONSTRAINT DEFINED(TextImWeb) OR DEFINED(Dokument);
    END Dokument;

    ASSOCIATION HinweisWeitereDokumente =
      Ursprung -- {0..*} Dokument;
      Hinweis -- {0..*} Dokument;
    END HinweisWeitereDokumente;

    ASSOCIATION RechtsvorschriftGWSAreal =
      Rechtsvorschrift -- {0..*} Dokument;
      GWSAreal -- {0..*} GWSAreal;
    END RechtsvorschriftGWSAreal;

    ASSOCIATION RechtsvorschriftGWSZone =
      Rechtsvorschrift -- {0..*} Dokument;
      GWSZone -- {0..*} GWSZone;
    END RechtsvorschriftGWSZone;

  END GWSZonen;

  TOPIC TransferMetadaten =

    /** Eine organisatorische Einheit innerhalb der öffentlichen Verwaltung, z.B. eine für Geobasisdaten zuständigen Stelle.
     */
    CLASS Amt =
      /** Name des Amtes z.B. "Amt für Gemeinden und Raumordnung des Kantons Bern".
       */
      Name : MANDATORY LocalisationCH_V1.MultilingualText;
      /** Verweis auf die Website des Amtes z.B. "http://www.jgk.be.ch/site/agr/".
       */
      AmtImWeb : URI;
      UID : TEXT*12;
    END Amt;

    /** Angaben zum Darstellungsdienst.
     */
    CLASS Darstellungsdienst =
      /** WMS GetMap-Request (für Maschine-Maschine-Kommunikation) inkl. alle benötigten Parameter, z.B. "http://ecogis.admin.ch/de/wms?version=1.1.1&service=WMS&request=GetMap&LAYERS=invent_leit&srs=EPSG:21781&WIDTH=500&HEIGHT=500&bbox=500000,100000,700000,300000&FORMAT=image/png&TRANSPARENT=TRUE"
       */
      VerweisWMS : URI;
      /** Verweis auf ein Dokument das die Karte beschreibt; z.B. "http://www.apps.be.ch/geoportal/output/gdp_adm_x3012app0081524772611.png"
       */
      LegendeImWeb : URI;
    END Darstellungsdienst;

    CLASS Datenbestand =
      BasketId : MANDATORY TEXT;
      Stand : MANDATORY INTERLIS.XMLDate;
      Lieferdatum : INTERLIS.XMLDate;
      Bemerkungen : MTEXT;
      /** Verweis auf weitere maschinenlesbare Metadaten (XML). z.B. http://www.geocat.ch/geonetwork/srv/deu/gm03.xml?id=705
       */
      weitereMetadaten : URI;
    END Datenbestand;

    ASSOCIATION zustaendigeStelleDatenbestand =
      zustaendigeStelle -- {1} Amt;
      Datenbestand -<> {0..*} Datenbestand;
    END zustaendigeStelleDatenbestand;

    ASSOCIATION DarstellungsdienstDatenbestand =
      Darstellungsdienst -- {1} Darstellungsdienst;
      Datenbestand -<> {0..*} Datenbestand;
    END DarstellungsdienstDatenbestand;

  END TransferMetadaten;

END PlanerischerGewaesserschutz_LV03_V1_1.

!!@ technicalContact=mailto:gis@bafu.admin.ch
!!@ furtherInformation=https://www.bafu.admin.ch/geodatenmodelle
!!@ IDGeoIV="130.1,131.1,132.1"
MODEL PlanerischerGewaesserschutz_LV95_V1_1 (de)
AT "https://models.geo.admin.ch/BAFU/"
VERSION "2017-06-20"  =
  IMPORTS LocalisationCH_V1,CHAdminCodes_V1,Units,GeometryCHLV95_V1;

  UNIT

    CubicMeterPerSecond [m3sec] = (Units.m3 / INTERLIS.s);

  DOMAIN

    Menge = 0 .. 100000 [m3sec];

    RechtsstatusArt = (
      inKraft,
      provisorisch
    );

    CHSurface = SURFACE WITH (STRAIGHTS) VERTEX GeometryCHLV95_V1.Coord2 WITHOUT OVERLAPS > 0.001;

  TOPIC GSBereiche =

    DOMAIN

      GSBereichTyp = (
        Ao,
        Au,
        Zo,
        Zu,
        UB
      );

    CLASS GSBereich =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV95_V1_1.CHSurface;
      Typ : MANDATORY GSBereichTyp;
      KantonaleTypBezeichnung : LocalisationCH_V1.LocalisedText;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
    END GSBereich;

  END GSBereiche;

  TOPIC GWSZonen =

    DOMAIN

      SchutzarealTyp = (
        Areal,
        ZukuenftigeZoneS1,
        ZukuenftigeZoneS2,
        ZukuenftigeZoneS3,
        ZukuenftigeZoneSh,
        ZukuenftigeZoneSm
      );

      SchutzzoneTyp = (
        S1,
        S2,
        S3,
        S3Zu,
        S_kantonaleArt,
        Sh,
        Sm
      );

      DokumentArt = (
        Rechtsvorschrift,
        GesetzlicheGrundlage,
        Hinweis
      );

    STRUCTURE Kanton_ = 
      Value: CHAdminCodes_V1.CHCantonCode;
    END Kanton_;

    /** Rechtskraftdatum ist MANDATORY falls Rechtsstatus = "inKraft"
     */
    CLASS Status =
      Rechtsstatus : MANDATORY PlanerischerGewaesserschutz_LV95_V1_1.RechtsstatusArt;
      Rechtskraftdatum : INTERLIS.XMLDate;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      KantonalerStatus : LocalisationCH_V1.LocalisedText;
      MANDATORY CONSTRAINT Rechtsstatus!=#inKraft OR DEFINED(Rechtskraftdatum);
    END Status;

    CLASS GWSAreal =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV95_V1_1.CHSurface;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      Typ : MANDATORY SchutzarealTyp;
      istAltrechtlich : MANDATORY BOOLEAN;
    END GWSAreal;

    CLASS GWSZone =
      Identifikator : INTERLIS.STANDARDOID;
      Geometrie : MANDATORY PlanerischerGewaesserschutz_LV95_V1_1.CHSurface;
      Bemerkungen : LocalisationCH_V1.LocalisedMText;
      Typ : MANDATORY SchutzzoneTyp;
      KantonaleTypBezeichnung : LocalisationCH_V1.LocalisedText;
      istAltrechtlich : MANDATORY BOOLEAN;
    END GWSZone;

    ASSOCIATION StatusGWSAreal =
      Status -- {1} Status;
      GWSAreal -<> {0..*} GWSAreal;
    END StatusGWSAreal;

    ASSOCIATION StatusGWSZone =
      Status -- {1} Status;
      GWSZone -<> {0..*} GWSZone;
    END StatusGWSZone;
    

    CLASS Dokument =
      Art : MANDATORY DokumentArt;
      Titel : MANDATORY TEXT*80;
      OffiziellerTitel : TEXT;
      Abkuerzung : TEXT*10;
      OffizielleNr : TEXT*20;
      Kanton : CHAdminCodes_V1.CHCantonCode;
      WeitereKantone : BAG {0..*} OF Kanton_;
      Gemeinde : CHAdminCodes_V1.CHMunicipalityCode;
      publiziertAb : MANDATORY INTERLIS.XMLDate;
      Rechtsstatus : MANDATORY PlanerischerGewaesserschutz_LV95_V1_1.RechtsstatusArt;
      TextImWeb : URI;
      /** Das Dokument als PDF-Datei
       */
      Dokument : BLACKBOX BINARY;
      MANDATORY CONSTRAINT DEFINED(TextImWeb) OR DEFINED(Dokument);
    END Dokument;

    ASSOCIATION HinweisWeitereDokumente =
      Ursprung -- {0..*} Dokument;
      Hinweis -- {0..*} Dokument;
    END HinweisWeitereDokumente;

    ASSOCIATION RechtsvorschriftGWSAreal =
      Rechtsvorschrift -- {0..*} Dokument;
      GWSAreal -- {0..*} GWSAreal;
    END RechtsvorschriftGWSAreal;

    ASSOCIATION RechtsvorschriftGWSZone =
      Rechtsvorschrift -- {0..*} Dokument;
      GWSZone -- {0..*} GWSZone;
    END RechtsvorschriftGWSZone;

  END GWSZonen;

  TOPIC TransferMetadaten =

    /** Eine organisatorische Einheit innerhalb der öffentlichen Verwaltung, z.B. eine für Geobasisdaten zuständigen Stelle.
     */
    CLASS Amt =
      /** Name des Amtes z.B. "Amt für Gemeinden und Raumordnung des Kantons Bern".
       */
      Name : MANDATORY LocalisationCH_V1.MultilingualText;
      /** Verweis auf die Website des Amtes z.B. "http://www.jgk.be.ch/site/agr/".
       */
      AmtImWeb : URI;
      UID : TEXT*12;
    END Amt;

    /** Angaben zum Darstellungsdienst.
     */
    CLASS Darstellungsdienst =
      /** WMS GetMap-Request (für Maschine-Maschine-Kommunikation) inkl. alle benötigten Parameter, z.B. "http://ecogis.admin.ch/de/wms?version=1.1.1&service=WMS&request=GetMap&LAYERS=invent_leit&srs=EPSG:21781&WIDTH=500&HEIGHT=500&bbox=500000,100000,700000,300000&FORMAT=image/png&TRANSPARENT=TRUE"
       */
      VerweisWMS : URI;
      /** Verweis auf ein Dokument das die Karte beschreibt; z.B. "http://www.apps.be.ch/geoportal/output/gdp_adm_x3012app0081524772611.png"
       */
      LegendeImWeb : URI;
    END Darstellungsdienst;

    CLASS Datenbestand =
      BasketId : MANDATORY TEXT;
      Stand : MANDATORY INTERLIS.XMLDate;
      Lieferdatum : INTERLIS.XMLDate;
      Bemerkungen : MTEXT;
      /** Verweis auf weitere maschinenlesbare Metadaten (XML). z.B. http://www.geocat.ch/geonetwork/srv/deu/gm03.xml?id=705
       */
      weitereMetadaten : URI;
    END Datenbestand;

    ASSOCIATION zustaendigeStelleDatenbestand =
      zustaendigeStelle -- {1} Amt;
      Datenbestand -<> {0..*} Datenbestand;
    END zustaendigeStelleDatenbestand;

    ASSOCIATION DarstellungsdienstDatenbestand =
      Darstellungsdienst -- {1} Darstellungsdienst;
      Datenbestand -<> {0..*} Datenbestand;
    END DarstellungsdienstDatenbestand;

  END TransferMetadaten;

END PlanerischerGewaesserschutz_LV95_V1_1.
','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('CHBase_Part2_LOCALISATION_20110830.ili','2.3','InternationalCodes_V1 Localisation_V1{ InternationalCodes_V1} LocalisationCH_V1{ InternationalCodes_V1 Localisation_V1} Dictionaries_V1{ InternationalCodes_V1} DictionariesCH_V1{ Dictionaries_V1 InternationalCodes_V1}','/* ########################################################################
   CHBASE - BASE MODULES OF THE SWISS FEDERATION FOR MINIMAL GEODATA MODELS
   ======
   BASISMODULE DES BUNDES           MODULES DE BASE DE LA CONFEDERATION
   FÜR MINIMALE GEODATENMODELLE     POUR LES MODELES DE GEODONNEES MINIMAUX
   
   PROVIDER: GKG/KOGIS - GCS/COSIG             CONTACT: models@geo.admin.ch
   PUBLISHED: 2011-08-30
   ########################################################################
*/

INTERLIS 2.3;

/* ########################################################################
   ########################################################################
   PART II -- LOCALISATION
   - Package InternationalCodes
   - Packages Localisation, LocalisationCH
   - Packages Dictionaries, DictionariesCH
*/

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
TYPE MODEL InternationalCodes_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  DOMAIN
    LanguageCode_ISO639_1 = (de,fr,it,rm,en,
      aa,ab,af,am,ar,as,ay,az,ba,be,bg,bh,bi,bn,bo,br,ca,co,cs,cy,da,dz,el,
      eo,es,et,eu,fa,fi,fj,fo,fy,ga,gd,gl,gn,gu,ha,he,hi,hr,hu,hy,ia,id,ie,
      ik,is,iu,ja,jw,ka,kk,kl,km,kn,ko,ks,ku,ky,la,ln,lo,lt,lv,mg,mi,mk,ml,
      mn,mo,mr,ms,mt,my,na,ne,nl,no,oc,om,or,pa,pl,ps,pt,qu,rn,ro,ru,rw,sa,
      sd,sg,sh,si,sk,sl,sm,sn,so,sq,sr,ss,st,su,sv,sw,ta,te,tg,th,ti,tk,tl,
      tn,to,tr,ts,tt,tw,ug,uk,ur,uz,vi,vo,wo,xh,yi,yo,za,zh,zu);

    CountryCode_ISO3166_1 = (CHE,
      ABW,AFG,AGO,AIA,ALA,ALB,AND_,ANT,ARE,ARG,ARM,ASM,ATA,ATF,ATG,AUS,
      AUT,AZE,BDI,BEL,BEN,BFA,BGD,BGR,BHR,BHS,BIH,BLR,BLZ,BMU,BOL,BRA,
      BRB,BRN,BTN,BVT,BWA,CAF,CAN,CCK,CHL,CHN,CIV,CMR,COD,COG,COK,COL,
      COM,CPV,CRI,CUB,CXR,CYM,CYP,CZE,DEU,DJI,DMA,DNK,DOM,DZA,ECU,EGY,
      ERI,ESH,ESP,EST,ETH,FIN,FJI,FLK,FRA,FRO,FSM,GAB,GBR,GEO,GGY,GHA,
      GIB,GIN,GLP,GMB,GNB,GNQ,GRC,GRD,GRL,GTM,GUF,GUM,GUY,HKG,HMD,HND,
      HRV,HTI,HUN,IDN,IMN,IND,IOT,IRL,IRN,IRQ,ISL,ISR,ITA,JAM,JEY,JOR,
      JPN,KAZ,KEN,KGZ,KHM,KIR,KNA,KOR,KWT,LAO,LBN,LBR,LBY,LCA,LIE,LKA,
      LSO,LTU,LUX,LVA,MAC,MAR,MCO,MDA,MDG,MDV,MEX,MHL,MKD,MLI,MLT,MMR,
      MNE,MNG,MNP,MOZ,MRT,MSR,MTQ,MUS,MWI,MYS,MYT,NAM,NCL,NER,NFK,NGA,
      NIC,NIU,NLD,NOR,NPL,NRU,NZL,OMN,PAK,PAN,PCN,PER,PHL,PLW,PNG,POL,
      PRI,PRK,PRT,PRY,PSE,PYF,QAT,REU,ROU,RUS,RWA,SAU,SDN,SEN,SGP,SGS,
      SHN,SJM,SLB,SLE,SLV,SMR,SOM,SPM,SRB,STP,SUR,SVK,SVN,SWE,SWZ,SYC,
      SYR,TCA,TCD,TGO,THA,TJK,TKL,TKM,TLS,TON,TTO,TUN,TUR,TUV,TWN,TZA,
      UGA,UKR,UMI,URY,USA,UZB,VAT,VCT,VEN,VGB,VIR,VNM,VUT,WLF,WSM,YEM,
      ZAF,ZMB,ZWE);

END InternationalCodes_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
TYPE MODEL Localisation_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED InternationalCodes_V1;

  STRUCTURE LocalisedText =
    Language: LanguageCode_ISO639_1;
    Text: MANDATORY TEXT;
  END LocalisedText;
  
  STRUCTURE LocalisedMText =
    Language: LanguageCode_ISO639_1;
    Text: MANDATORY MTEXT;
  END LocalisedMText;

  STRUCTURE MultilingualText =
    LocalisedText : BAG {1..*} OF LocalisedText;
    UNIQUE (LOCAL) LocalisedText:Language;
  END MultilingualText;  
  
  STRUCTURE MultilingualMText =
    LocalisedText : BAG {1..*} OF LocalisedMText;
    UNIQUE (LOCAL) LocalisedText:Language;
  END MultilingualMText;

END Localisation_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
TYPE MODEL LocalisationCH_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED InternationalCodes_V1;
  IMPORTS Localisation_V1;

  STRUCTURE LocalisedText EXTENDS Localisation_V1.LocalisedText =
  MANDATORY CONSTRAINT
    Language == #de OR
    Language == #fr OR
    Language == #it OR
    Language == #rm OR
    Language == #en;
  END LocalisedText;
  
  STRUCTURE LocalisedMText EXTENDS Localisation_V1.LocalisedMText =
  MANDATORY CONSTRAINT
    Language == #de OR
    Language == #fr OR
    Language == #it OR
    Language == #rm OR
    Language == #en;
  END LocalisedMText;

  STRUCTURE MultilingualText EXTENDS Localisation_V1.MultilingualText =
    LocalisedText(EXTENDED) : BAG {1..*} OF LocalisedText;
  END MultilingualText;  
  
  STRUCTURE MultilingualMText EXTENDS Localisation_V1.MultilingualMText =
    LocalisedText(EXTENDED) : BAG {1..*} OF LocalisedMText;
  END MultilingualMText;

END LocalisationCH_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
MODEL Dictionaries_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED InternationalCodes_V1;

  TOPIC Dictionaries (ABSTRACT) =

    STRUCTURE Entry (ABSTRACT) =
      Text: MANDATORY TEXT;
    END Entry;
      
    CLASS Dictionary =
      Language: MANDATORY LanguageCode_ISO639_1;
      Entries: LIST OF Entry;
    END Dictionary;

  END Dictionaries;

END Dictionaries_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
MODEL DictionariesCH_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED InternationalCodes_V1;
  IMPORTS Dictionaries_V1;

  TOPIC Dictionaries (ABSTRACT) EXTENDS Dictionaries_V1.Dictionaries =

    CLASS Dictionary (EXTENDED) =
    MANDATORY CONSTRAINT
      Language == #de OR
      Language == #fr OR
      Language == #it OR
      Language == #rm OR
      Language == #en;
    END Dictionary;

  END Dictionaries;

END DictionariesCH_V1.

!! ########################################################################
','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('CoordSys-20151124.ili','2.3','CoordSys','!! File CoordSys.ili Release 2015-11-24

INTERLIS 2.3;

!! 2015-11-24 Cardinalities adapted (line 122, 123, 124, 132, 133, 134, 142, 143,
!!                                   148, 149, 163, 164, 168, 169, 206 and 207)
!!@precursorVersion = 2005-06-16

REFSYSTEM MODEL CoordSys (en) AT "http://www.interlis.ch/models"
  VERSION "2015-11-24" =

  UNIT
    Angle_Degree = 180 / PI [INTERLIS.rad];
    Angle_Minute = 1 / 60 [Angle_Degree];
    Angle_Second = 1 / 60 [Angle_Minute];

  STRUCTURE Angle_DMS_S =
    Degrees: -180 .. 180 CIRCULAR [Angle_Degree];
    CONTINUOUS SUBDIVISION Minutes: 0 .. 59 CIRCULAR [Angle_Minute];
    CONTINUOUS SUBDIVISION Seconds: 0.000 .. 59.999 CIRCULAR [Angle_Second];
  END Angle_DMS_S;

  DOMAIN
    Angle_DMS = FORMAT BASED ON Angle_DMS_S (Degrees ":" Minutes ":" Seconds);
    Angle_DMS_90 EXTENDS Angle_DMS = "-90:00:00.000" .. "90:00:00.000";


  TOPIC CoordsysTopic =

    !! Special space aspects to be referenced
    !! **************************************

    CLASS Ellipsoid EXTENDS INTERLIS.REFSYSTEM =
      EllipsoidAlias: TEXT*70;
      SemiMajorAxis: MANDATORY 6360000.0000 .. 6390000.0000 [INTERLIS.m];
      InverseFlattening: MANDATORY 0.00000000 .. 350.00000000;
      !! The inverse flattening 0 characterizes the 2-dim sphere
      Remarks: TEXT*70;
    END Ellipsoid;

    CLASS GravityModel EXTENDS INTERLIS.REFSYSTEM =
      GravityModAlias: TEXT*70;
      Definition: TEXT*70;
    END GravityModel;

    CLASS GeoidModel EXTENDS INTERLIS.REFSYSTEM =
      GeoidModAlias: TEXT*70;
      Definition: TEXT*70;
    END GeoidModel;


    !! Coordinate systems for geodetic purposes
    !! ****************************************

    STRUCTURE LengthAXIS EXTENDS INTERLIS.AXIS =
      ShortName: TEXT*12;
      Description: TEXT*255;
    PARAMETER
      Unit (EXTENDED): NUMERIC [INTERLIS.LENGTH];
    END LengthAXIS;

    STRUCTURE AngleAXIS EXTENDS INTERLIS.AXIS =
      ShortName: TEXT*12;
      Description: TEXT*255;
    PARAMETER
      Unit (EXTENDED): NUMERIC [INTERLIS.ANGLE];
    END AngleAXIS;

    CLASS GeoCartesian1D EXTENDS INTERLIS.COORDSYSTEM =
      Axis (EXTENDED): LIST {1} OF LengthAXIS;
    END GeoCartesian1D;

    CLASS GeoHeight EXTENDS GeoCartesian1D =
      System: MANDATORY (
        normal,
        orthometric,
        ellipsoidal,
        other);
      ReferenceHeight: MANDATORY -10000.000 .. +10000.000 [INTERLIS.m];
      ReferenceHeightDescr: TEXT*70;
    END GeoHeight;

    ASSOCIATION HeightEllips =
      GeoHeightRef -- {*} GeoHeight;
      EllipsoidRef -- {1} Ellipsoid;
    END HeightEllips;

    ASSOCIATION HeightGravit =
      GeoHeightRef -- {*} GeoHeight;
      GravityRef -- {1} GravityModel;
    END HeightGravit;

    ASSOCIATION HeightGeoid =
      GeoHeightRef -- {*} GeoHeight;
      GeoidRef -- {1} GeoidModel;
    END HeightGeoid;

    CLASS GeoCartesian2D EXTENDS INTERLIS.COORDSYSTEM =
      Definition: TEXT*70;
      Axis (EXTENDED): LIST {2} OF LengthAXIS;
    END GeoCartesian2D;

    CLASS GeoCartesian3D EXTENDS INTERLIS.COORDSYSTEM =
      Definition: TEXT*70;
      Axis (EXTENDED): LIST {3} OF LengthAXIS;
    END GeoCartesian3D;

    CLASS GeoEllipsoidal EXTENDS INTERLIS.COORDSYSTEM =
      Definition: TEXT*70;
      Axis (EXTENDED): LIST {2} OF AngleAXIS;
    END GeoEllipsoidal;

    ASSOCIATION EllCSEllips =
      GeoEllipsoidalRef -- {*} GeoEllipsoidal;
      EllipsoidRef -- {1} Ellipsoid;
    END EllCSEllips;


    !! Mappings between coordinate systems
    !! ***********************************

    ASSOCIATION ToGeoEllipsoidal =
      From -- {0..*} GeoCartesian3D;
      To -- {0..*} GeoEllipsoidal;
      ToHeight -- {0..*} GeoHeight;
    MANDATORY CONSTRAINT
      ToHeight -> System == #ellipsoidal;
    MANDATORY CONSTRAINT
      To -> EllipsoidRef -> Name == ToHeight -> EllipsoidRef -> Name;
    END ToGeoEllipsoidal;

    ASSOCIATION ToGeoCartesian3D =
      From2 -- {0..*} GeoEllipsoidal;
      FromHeight-- {0..*} GeoHeight;
      To3 -- {0..*} GeoCartesian3D;
    MANDATORY CONSTRAINT
      FromHeight -> System == #ellipsoidal;
    MANDATORY CONSTRAINT
      From2 -> EllipsoidRef -> Name == FromHeight -> EllipsoidRef -> Name;
    END ToGeoCartesian3D;

    ASSOCIATION BidirectGeoCartesian2D =
      From -- {0..*} GeoCartesian2D;
      To -- {0..*} GeoCartesian2D;
    END BidirectGeoCartesian2D;

    ASSOCIATION BidirectGeoCartesian3D =
      From -- {0..*} GeoCartesian3D;
      To2 -- {0..*} GeoCartesian3D;
      Precision: MANDATORY (
        exact,
        measure_based);
      ShiftAxis1: MANDATORY -10000.000 .. 10000.000 [INTERLIS.m];
      ShiftAxis2: MANDATORY -10000.000 .. 10000.000 [INTERLIS.m];
      ShiftAxis3: MANDATORY -10000.000 .. 10000.000 [INTERLIS.m];
      RotationAxis1: Angle_DMS_90;
      RotationAxis2: Angle_DMS_90;
      RotationAxis3: Angle_DMS_90;
      NewScale: 0.000001 .. 1000000.000000;
    END BidirectGeoCartesian3D;

    ASSOCIATION BidirectGeoEllipsoidal =
      From4 -- {0..*} GeoEllipsoidal;
      To4 -- {0..*} GeoEllipsoidal;
    END BidirectGeoEllipsoidal;

    ASSOCIATION MapProjection (ABSTRACT) =
      From5 -- {0..*} GeoEllipsoidal;
      To5 -- {0..*} GeoCartesian2D;
      FromCo1_FundPt: MANDATORY Angle_DMS_90;
      FromCo2_FundPt: MANDATORY Angle_DMS_90;
      ToCoord1_FundPt: MANDATORY -10000000 .. +10000000 [INTERLIS.m];
      ToCoord2_FundPt: MANDATORY -10000000 .. +10000000 [INTERLIS.m];
    END MapProjection;

    ASSOCIATION TransverseMercator EXTENDS MapProjection =
    END TransverseMercator;

    ASSOCIATION SwissProjection EXTENDS MapProjection =
      IntermFundP1: MANDATORY Angle_DMS_90;
      IntermFundP2: MANDATORY Angle_DMS_90;
    END SwissProjection;

    ASSOCIATION Mercator EXTENDS MapProjection =
    END Mercator;

    ASSOCIATION ObliqueMercator EXTENDS MapProjection =
    END ObliqueMercator;

    ASSOCIATION Lambert EXTENDS MapProjection =
    END Lambert;

    ASSOCIATION Polyconic EXTENDS MapProjection =
    END Polyconic;

    ASSOCIATION Albus EXTENDS MapProjection =
    END Albus;

    ASSOCIATION Azimutal EXTENDS MapProjection =
    END Azimutal;

    ASSOCIATION Stereographic EXTENDS MapProjection =
    END Stereographic;

    ASSOCIATION HeightConversion =
      FromHeight -- {0..*} GeoHeight;
      ToHeight -- {0..*} GeoHeight;
      Definition: TEXT*70;
    END HeightConversion;

  END CoordsysTopic;

END CoordSys.

','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('CHBase_Part4_ADMINISTRATIVEUNITS_20110830.ili','2.3','CHAdminCodes_V1 AdministrativeUnits_V1{ Dictionaries_V1 INTERLIS CHAdminCodes_V1 InternationalCodes_V1 Localisation_V1} AdministrativeUnitsCH_V1{ LocalisationCH_V1 INTERLIS CHAdminCodes_V1 InternationalCodes_V1 AdministrativeUnits_V1}','/* ########################################################################
   CHBASE - BASE MODULES OF THE SWISS FEDERATION FOR MINIMAL GEODATA MODELS
   ======
   BASISMODULE DES BUNDES           MODULES DE BASE DE LA CONFEDERATION
   FÜR MINIMALE GEODATENMODELLE     POUR LES MODELES DE GEODONNEES MINIMAUX
   
   PROVIDER: GKG/KOGIS - GCS/COSIG             CONTACT: models@geo.admin.ch
   PUBLISHED: 2011-08-30
   ########################################################################
*/

INTERLIS 2.3;

/* ########################################################################
   ########################################################################
   PART IV -- ADMINISTRATIVE UNITS
   - Package CHAdminCodes
   - Package AdministrativeUnits
   - Package AdministrativeUnitsCH
*/

!! Version    | Who   | Modification
!!------------------------------------------------------------------------------
!! 2018-02-19 | KOGIS | CHCantonCode adapted (FL and CH added) (line 34)

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=https://www.geo.admin.ch/de/geoinformation-schweiz/geobasisdaten/geodata-models.html
TYPE MODEL CHAdminCodes_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2018-02-19" =

  DOMAIN
    CHCantonCode = (ZH,BE,LU,UR,SZ,OW,NW,GL,ZG,FR,SO,BS,BL,SH,AR,AI,SG,
                    GR,AG,TG,TI,VD,VS,NE,GE,JU,FL,CH);

    CHMunicipalityCode = 1..9999;  !! BFS-Nr

END CHAdminCodes_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
MODEL AdministrativeUnits_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED INTERLIS;
  IMPORTS UNQUALIFIED CHAdminCodes_V1;
  IMPORTS UNQUALIFIED InternationalCodes_V1;
  IMPORTS Localisation_V1;
  IMPORTS Dictionaries_V1;

  TOPIC AdministrativeUnits (ABSTRACT) =

    CLASS AdministrativeElement (ABSTRACT) =
    END AdministrativeElement;

    CLASS AdministrativeUnit (ABSTRACT) EXTENDS AdministrativeElement =
    END AdministrativeUnit;

    ASSOCIATION Hierarchy =
      UpperLevelUnit (EXTERNAL) -<> {0..1} AdministrativeUnit;
      LowerLevelUnit -- AdministrativeUnit;
    END Hierarchy;

    CLASS AdministrativeUnion (ABSTRACT) EXTENDS AdministrativeElement =
    END AdministrativeUnion;

    ASSOCIATION UnionMembers =
      Union -<> AdministrativeUnion;
      Member -- AdministrativeElement; 
    END UnionMembers;

  END AdministrativeUnits;

  TOPIC Countries EXTENDS AdministrativeUnits =

    CLASS Country EXTENDS AdministrativeUnit =
      Code: MANDATORY CountryCode_ISO3166_1;
    UNIQUE Code;
    END Country;

  END Countries;

  TOPIC CountryNames EXTENDS Dictionaries_V1.Dictionaries =
    DEPENDS ON AdministrativeUnits_V1.Countries;

    STRUCTURE CountryName EXTENDS Entry =
      Code: MANDATORY CountryCode_ISO3166_1;
    END CountryName;
      
    CLASS CountryNamesTranslation EXTENDS Dictionary  =
      Entries(EXTENDED): LIST OF CountryName;
    UNIQUE Entries->Code;
    EXISTENCE CONSTRAINT
      Entries->Code REQUIRED IN AdministrativeUnits_V1.Countries.Country: Code;
    END CountryNamesTranslation;

  END CountryNames;

  TOPIC Agencies (ABSTRACT) =
    DEPENDS ON AdministrativeUnits_V1.AdministrativeUnits;

    CLASS Agency (ABSTRACT) =
    END Agency;

    ASSOCIATION Authority =
      Supervisor (EXTERNAL) -<> {1..1} Agency OR AdministrativeUnits_V1.AdministrativeUnits.AdministrativeElement;
      Agency -- Agency;
    END Authority;

    ASSOCIATION Organisation =
      Orderer (EXTERNAL) -- Agency OR AdministrativeUnits_V1.AdministrativeUnits.AdministrativeElement;
      Executor -- Agency;
    END Organisation;

  END Agencies;

END AdministrativeUnits_V1.

!! ########################################################################
!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html
MODEL AdministrativeUnitsCH_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2011-08-30" =

  IMPORTS UNQUALIFIED INTERLIS;
  IMPORTS UNQUALIFIED CHAdminCodes_V1;
  IMPORTS UNQUALIFIED InternationalCodes_V1;
  IMPORTS LocalisationCH_V1;
  IMPORTS AdministrativeUnits_V1;

  TOPIC CHCantons EXTENDS AdministrativeUnits_V1.AdministrativeUnits =
    DEPENDS ON AdministrativeUnits_V1.Countries;

    CLASS CHCanton EXTENDS AdministrativeUnit =
      Code: MANDATORY CHCantonCode;
      Name: LocalisationCH_V1.MultilingualText;
      Web: URI;
    UNIQUE Code;
    END CHCanton;

    ASSOCIATION Hierarchy (EXTENDED) =
      UpperLevelUnit (EXTENDED, EXTERNAL) -<> {1..1} AdministrativeUnits_V1.Countries.Country;
      LowerLevelUnit (EXTENDED) -- CHCanton;
    MANDATORY CONSTRAINT
      UpperLevelUnit->Code == "CHE";
    END Hierarchy;

  END CHCantons;

  TOPIC CHDistricts EXTENDS AdministrativeUnits_V1.AdministrativeUnits =
    DEPENDS ON AdministrativeUnitsCH_V1.CHCantons;

    CLASS CHDistrict EXTENDS AdministrativeUnit =
      ShortName: MANDATORY TEXT*20;
      Name: LocalisationCH_V1.MultilingualText;
      Web: MANDATORY URI;
    END CHDistrict;

    ASSOCIATION Hierarchy (EXTENDED) =
      UpperLevelUnit (EXTENDED, EXTERNAL) -<> {1..1} AdministrativeUnitsCH_V1.CHCantons.CHCanton;
      LowerLevelUnit (EXTENDED) -- CHDistrict;
    UNIQUE UpperLevelUnit->Code, LowerLevelUnit->ShortName;
    END Hierarchy;

  END CHDistricts;

  TOPIC CHMunicipalities EXTENDS AdministrativeUnits_V1.AdministrativeUnits =
    DEPENDS ON AdministrativeUnitsCH_V1.CHCantons;
    DEPENDS ON AdministrativeUnitsCH_V1.CHDistricts;

    CLASS CHMunicipality EXTENDS AdministrativeUnit =
      Code: MANDATORY CHMunicipalityCode;
      Name: LocalisationCH_V1.MultilingualText;
      Web: URI;
    UNIQUE Code;
    END CHMunicipality;

    ASSOCIATION Hierarchy (EXTENDED) =
      UpperLevelUnit (EXTENDED, EXTERNAL) -<> {1..1} AdministrativeUnitsCH_V1.CHCantons.CHCanton
      OR AdministrativeUnitsCH_V1.CHDistricts.CHDistrict;
      LowerLevelUnit (EXTENDED) -- CHMunicipality;
    END Hierarchy;

  END CHMunicipalities;

  TOPIC CHAdministrativeUnions EXTENDS AdministrativeUnits_V1.AdministrativeUnits =
    DEPENDS ON AdministrativeUnits_V1.Countries;
    DEPENDS ON AdministrativeUnitsCH_V1.CHCantons;
    DEPENDS ON AdministrativeUnitsCH_V1.CHDistricts;
    DEPENDS ON AdministrativeUnitsCH_V1.CHMunicipalities;

    CLASS AdministrativeUnion (EXTENDED) =
    OID AS UUIDOID;
      Name: LocalisationCH_V1.MultilingualText;
      Web: URI;
      Description: LocalisationCH_V1.MultilingualMText;
    END AdministrativeUnion;

  END CHAdministrativeUnions;

  TOPIC CHAgencies EXTENDS AdministrativeUnits_V1.Agencies =
    DEPENDS ON AdministrativeUnits_V1.Countries;
    DEPENDS ON AdministrativeUnitsCH_V1.CHCantons;
    DEPENDS ON AdministrativeUnitsCH_V1.CHDistricts;
    DEPENDS ON AdministrativeUnitsCH_V1.CHMunicipalities;

    CLASS Agency (EXTENDED) =
    OID AS UUIDOID;
      Name: LocalisationCH_V1.MultilingualText;
      Web: URI;
      Description: LocalisationCH_V1.MultilingualMText;
    END Agency;

  END CHAgencies;

END AdministrativeUnitsCH_V1.

!! ########################################################################
','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('Units-20120220.ili','2.3','Units','!! File Units.ili Release 2012-02-20

INTERLIS 2.3;

!! 2012-02-20 definition of "Bar [bar]" corrected
!!@precursorVersion = 2005-06-06

CONTRACTED TYPE MODEL Units (en) AT "http://www.interlis.ch/models"
  VERSION "2012-02-20" =

  UNIT
    !! abstract Units
    Area (ABSTRACT) = (INTERLIS.LENGTH*INTERLIS.LENGTH);
    Volume (ABSTRACT) = (INTERLIS.LENGTH*INTERLIS.LENGTH*INTERLIS.LENGTH);
    Velocity (ABSTRACT) = (INTERLIS.LENGTH/INTERLIS.TIME);
    Acceleration (ABSTRACT) = (Velocity/INTERLIS.TIME);
    Force (ABSTRACT) = (INTERLIS.MASS*INTERLIS.LENGTH/INTERLIS.TIME/INTERLIS.TIME);
    Pressure (ABSTRACT) = (Force/Area);
    Energy (ABSTRACT) = (Force*INTERLIS.LENGTH);
    Power (ABSTRACT) = (Energy/INTERLIS.TIME);
    Electric_Potential (ABSTRACT) = (Power/INTERLIS.ELECTRIC_CURRENT);
    Frequency (ABSTRACT) = (INTERLIS.DIMENSIONLESS/INTERLIS.TIME);

    Millimeter [mm] = 0.001 [INTERLIS.m];
    Centimeter [cm] = 0.01 [INTERLIS.m];
    Decimeter [dm] = 0.1 [INTERLIS.m];
    Kilometer [km] = 1000 [INTERLIS.m];

    Square_Meter [m2] EXTENDS Area = (INTERLIS.m*INTERLIS.m);
    Cubic_Meter [m3] EXTENDS Volume = (INTERLIS.m*INTERLIS.m*INTERLIS.m);

    Minute [min] = 60 [INTERLIS.s];
    Hour [h] = 60 [min];
    Day [d] = 24 [h];

    Kilometer_per_Hour [kmh] EXTENDS Velocity = (km/h);
    Meter_per_Second [ms] = 3.6 [kmh];
    Newton [N] EXTENDS Force = (INTERLIS.kg*INTERLIS.m/INTERLIS.s/INTERLIS.s);
    Pascal [Pa] EXTENDS Pressure = (N/m2);
    Joule [J] EXTENDS Energy = (N*INTERLIS.m);
    Watt [W] EXTENDS Power = (J/INTERLIS.s);
    Volt [V] EXTENDS Electric_Potential = (W/INTERLIS.A);

    Inch [in] = 2.54 [cm];
    Foot [ft] = 0.3048 [INTERLIS.m];
    Mile [mi] = 1.609344 [km];

    Are [a] = 100 [m2];
    Hectare [ha] = 100 [a];
    Square_Kilometer [km2] = 100 [ha];
    Acre [acre] = 4046.873 [m2];

    Liter [L] = 1 / 1000 [m3];
    US_Gallon [USgal] = 3.785412 [L];

    Angle_Degree = 180 / PI [INTERLIS.rad];
    Angle_Minute = 1 / 60 [Angle_Degree];
    Angle_Second = 1 / 60 [Angle_Minute];

    Gon = 200 / PI [INTERLIS.rad];

    Gram [g] = 1 / 1000 [INTERLIS.kg];
    Ton [t] = 1000 [INTERLIS.kg];
    Pound [lb] = 0.4535924 [INTERLIS.kg];

    Calorie [cal] = 4.1868 [J];
    Kilowatt_Hour [kWh] = 0.36E7 [J];

    Horsepower = 746 [W];

    Techn_Atmosphere [at] = 98066.5 [Pa];
    Atmosphere [atm] = 101325 [Pa];
    Bar [bar] = 100000 [Pa];
    Millimeter_Mercury [mmHg] = 133.3224 [Pa];
    Torr = 133.3224 [Pa]; !! Torr = [mmHg]

    Decibel [dB] = FUNCTION // 10**(dB/20) * 0.00002 // [Pa];

    Degree_Celsius [oC] = FUNCTION // oC+273.15 // [INTERLIS.K];
    Degree_Fahrenheit [oF] = FUNCTION // (oF+459.67)/1.8 // [INTERLIS.K];

    CountedObjects EXTENDS INTERLIS.DIMENSIONLESS;

    Hertz [Hz] EXTENDS Frequency = (CountedObjects/INTERLIS.s);
    KiloHertz [KHz] = 1000 [Hz];
    MegaHertz [MHz] = 1000 [KHz];

    Percent = 0.01 [CountedObjects];
    Permille = 0.001 [CountedObjects];

    !! ISO 4217 Currency Abbreviation
    USDollar [USD] EXTENDS INTERLIS.MONEY;
    Euro [EUR] EXTENDS INTERLIS.MONEY;
    SwissFrancs [CHF] EXTENDS INTERLIS.MONEY;

END Units.

','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_MODEL (filename,iliversion,modelName,content,importDate) VALUES ('CHBase_Part1_GEOMETRY_20110830.ili','2.3','GeometryCHLV03_V1{ INTERLIS CoordSys Units} GeometryCHLV95_V1{ INTERLIS CoordSys Units}','/* ########################################################################
   CHBASE - BASE MODULES OF THE SWISS FEDERATION FOR MINIMAL GEODATA MODELS
   ======
   BASISMODULE DES BUNDES           MODULES DE BASE DE LA CONFEDERATION
   FÜR MINIMALE GEODATENMODELLE     POUR LES MODELES DE GEODONNEES MINIMAUX
   
   PROVIDER: GKG/KOGIS - GCS/COSIG             CONTACT: models@geo.admin.ch
   PUBLISHED: 2011-0830
   ########################################################################
*/

INTERLIS 2.3;

/* ########################################################################
   ########################################################################
   PART I -- GEOMETRY
   - Package GeometryCHLV03
   - Package GeometryCHLV95
*/

!! ########################################################################

!! Version    | Who   | Modification
!!------------------------------------------------------------------------------
!! 2015-02-20 | KOGIS | WITHOUT OVERLAPS added (line 57, 58, 65 and 66)
!! 2015-11-12 | KOGIS | WITHOUT OVERLAPS corrected (line 57 and 58)
!! 2017-11-27 | KOGIS | Meta-Attributes @furtherInformation adapted and @CRS added (line 31, 44 and 50)
!! 2017-12-04 | KOGIS | Meta-Attribute @CRS corrected

!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=https://www.geo.admin.ch/de/geoinformation-schweiz/geobasisdaten/geodata-models.html
TYPE MODEL GeometryCHLV03_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2017-12-04" =

  IMPORTS UNQUALIFIED INTERLIS;
  IMPORTS Units;
  IMPORTS CoordSys;

  REFSYSTEM BASKET BCoordSys ~ CoordSys.CoordsysTopic
    OBJECTS OF GeoCartesian2D: CHLV03
    OBJECTS OF GeoHeight: SwissOrthometricAlt;

  DOMAIN
    !!@CRS=EPSG:21781
    Coord2 = COORD
      460000.000 .. 870000.000 [m] {CHLV03[1]},
       45000.000 .. 310000.000 [m] {CHLV03[2]},
      ROTATION 2 -> 1;

    !!@CRS=EPSG:21781
    Coord3 = COORD
      460000.000 .. 870000.000 [m] {CHLV03[1]},
       45000.000 .. 310000.000 [m] {CHLV03[2]},
        -200.000 ..   5000.000 [m] {SwissOrthometricAlt[1]},
      ROTATION 2 -> 1;

    Surface = SURFACE WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.001;
    Area = AREA WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.001;
    Line = POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord2;
    DirectedLine EXTENDS Line = DIRECTED POLYLINE;
    LineWithAltitude = POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord3;
    DirectedLineWithAltitude = DIRECTED POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord3;
    
    /* minimal overlaps only (2mm) */
    SurfaceWithOverlaps2mm = SURFACE WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.002;
    AreaWithOverlaps2mm = AREA WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.002;

    Orientation = 0.00000 .. 359.99999 CIRCULAR [Units.Angle_Degree] <Coord2>;

    Accuracy = (cm, cm50, m, m10, m50, vague);
    Method = (measured, sketched, calculated);

    STRUCTURE LineStructure = 
      Line: Line;
    END LineStructure;

    STRUCTURE DirectedLineStructure =
      Line: DirectedLine;
    END DirectedLineStructure;

    STRUCTURE MultiLine =
      Lines: BAG {1..*} OF LineStructure;
    END MultiLine;

    STRUCTURE MultiDirectedLine =
      Lines: BAG {1..*} OF DirectedLineStructure;
    END MultiDirectedLine;

    STRUCTURE SurfaceStructure =
      Surface: Surface;
    END SurfaceStructure;

    STRUCTURE MultiSurface =
      Surfaces: BAG {1..*} OF SurfaceStructure;
    END MultiSurface;

END GeometryCHLV03_V1.

!! ########################################################################

!! Version    | Who   | Modification
!!------------------------------------------------------------------------------
!! 2015-02-20 | KOGIS | WITHOUT OVERLAPS added (line 135, 136, 143 and 144)
!! 2015-11-12 | KOGIS | WITHOUT OVERLAPS corrected (line 135 and 136)
!! 2017-11-27 | KOGIS | Meta-Attributes @furtherInformation adapted and @CRS added (line 109, 122 and 128)
!! 2017-12-04 | KOGIS | Meta-Attribute @CRS corrected

!!@technicalContact=models@geo.admin.ch
!!@furtherInformation=https://www.geo.admin.ch/de/geoinformation-schweiz/geobasisdaten/geodata-models.html
TYPE MODEL GeometryCHLV95_V1 (en)
  AT "http://www.geo.admin.ch" VERSION "2017-12-04" =

  IMPORTS UNQUALIFIED INTERLIS;
  IMPORTS Units;
  IMPORTS CoordSys;

  REFSYSTEM BASKET BCoordSys ~ CoordSys.CoordsysTopic
    OBJECTS OF GeoCartesian2D: CHLV95
    OBJECTS OF GeoHeight: SwissOrthometricAlt;

  DOMAIN
    !!@CRS=EPSG:2056
    Coord2 = COORD
      2460000.000 .. 2870000.000 [m] {CHLV95[1]},
      1045000.000 .. 1310000.000 [m] {CHLV95[2]},
      ROTATION 2 -> 1;

    !!@CRS=EPSG:2056
    Coord3 = COORD
      2460000.000 .. 2870000.000 [m] {CHLV95[1]},
      1045000.000 .. 1310000.000 [m] {CHLV95[2]},
         -200.000 ..   5000.000 [m] {SwissOrthometricAlt[1]},
      ROTATION 2 -> 1;

    Surface = SURFACE WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.001;
    Area = AREA WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.001;
    Line = POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord2;
    DirectedLine EXTENDS Line = DIRECTED POLYLINE;
    LineWithAltitude = POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord3;
    DirectedLineWithAltitude = DIRECTED POLYLINE WITH (STRAIGHTS, ARCS) VERTEX Coord3;
    
    /* minimal overlaps only (2mm) */
    SurfaceWithOverlaps2mm = SURFACE WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.002;
    AreaWithOverlaps2mm = AREA WITH (STRAIGHTS, ARCS) VERTEX Coord2 WITHOUT OVERLAPS > 0.002;

    Orientation = 0.00000 .. 359.99999 CIRCULAR [Units.Angle_Degree] <Coord2>;

    Accuracy = (cm, cm50, m, m10, m50, vague);
    Method = (measured, sketched, calculated);

    STRUCTURE LineStructure = 
      Line: Line;
    END LineStructure;

    STRUCTURE DirectedLineStructure =
      Line: DirectedLine;
    END DirectedLineStructure;

    STRUCTURE MultiLine =
      Lines: BAG {1..*} OF LineStructure;
    END MultiLine;

    STRUCTURE MultiDirectedLine =
      Lines: BAG {1..*} OF DirectedLineStructure;
    END MultiDirectedLine;

    STRUCTURE SurfaceStructure =
      Surface: Surface;
    END SurfaceStructure;

    STRUCTURE MultiSurface =
      Surfaces: BAG {1..*} OF SurfaceStructure;
    END MultiSurface;

END GeometryCHLV95_V1.

!! ########################################################################
','2019-12-05 12:01:21.193');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.createMetaInfo','True');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.multiPointTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.nameOptimization','topic');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.multiSurfaceTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.maxSqlNameLength','60');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.beautifyEnumDispName','underscore');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.multiLineTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.jsonTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.StrokeArcs','enable');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.createForeignKey','yes');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.uniqueConstraints','create');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.sqlgen.createGeomIndex','True');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.uuidDefaultValue','uuid_generate_v4()');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.multilingualTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.defaultSrsAuthority','EPSG');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.arrayTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.catalogueRefTrafo','coalesce');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.defaultSrsCode','2056');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.createForeignKeyIndex','yes');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.createEnumDefs','multiTable');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.inheritanceTrafo','smart1');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.sender','ili2pg-4.3.1-23b1f79e8ad644414773bb9bd1a97c8c265c5082');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.numericCheckConstraints','create');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.ehi.ili2db.localisedTrafo','expand');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_SETTINGS (tag,setting) VALUES ('ch.interlis.ili2c.ilidirs','http://models.geo.admin.ch/;http://geo.so.ch/models;');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV03_V1_1','technicalContact','mailto:gis@bafu.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV03_V1_1','furtherInformation','https://www.bafu.admin.ch/geodatenmodelle');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV03_V1_1','IDGeoIV','130.1,131.1,132.1');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('Dictionaries_V1','technicalContact','models@geo.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('Dictionaries_V1','furtherInformation','http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('AdministrativeUnits_V1','technicalContact','models@geo.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('AdministrativeUnits_V1','furtherInformation','http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('AdministrativeUnitsCH_V1','technicalContact','models@geo.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('AdministrativeUnitsCH_V1','furtherInformation','http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1','technicalContact','mailto:gis@bafu.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1','furtherInformation','https://www.bafu.admin.ch/geodatenmodelle');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('PlanerischerGewaesserschutz_LV95_V1_1','IDGeoIV','130.1,131.1,132.1');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('DictionariesCH_V1','technicalContact','models@geo.admin.ch');
INSERT INTO afu_gewaesserschutz.T_ILI2DB_META_ATTRS (ilielement,attr_name,attr_value) VALUES ('DictionariesCH_V1','furtherInformation','http://www.geo.admin.ch/internet/geoportal/de/home/topics/geobasedata/models.html');
