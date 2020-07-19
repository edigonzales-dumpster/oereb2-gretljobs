/* FRAGEN:
 * - Zusammenspiel Legendeneintrag <-> Darstellungsdienst <-> Eigentumsbeschränkung <-> Legendeneintrag
 * - Reicht die WMS-Layergruppe oder müssen die WMS-Einzellayer erfasst und verknüpft werden? Bei V1.1 reicht
 * die Layergruppe. Vielleicht hilft hier sogar auch, dass es für jeden Geometrietyp ein Symbol gibt.
 * - LEFT JOIN bei den Eigentumbeschränkungen mit dem Darstellungsdienst kann vielleicht erst ganz am Schluss
 * gemacht werden (über alle zusammen). Dann braucht es aber noch ein separates Join-Attribut (code, subcode).
 */

/*
 * Allgemeine Bemerkungen:
 *
 * Die korrekte Reihenfolge der Queries ist zwingend. 
 * 
 * Es wird versucht möglichst rasch die Daten in den Tabellen zu speichern. So
 * können die Queries gekapselt werden und/oder in nachvollziehbareren 
 * Teilschritten durchgeführt werden. Alternativ kann man (fast) alles in einer sehr
 * langen CTE umbauen.
 * 
 * Es wird versucht wenn immer möglich die Original-TID (resp. der PK aus der 
 * Quelltabelle) in der Zieltabelle beizubehalten. Damit bleiben Beziehungen
 * 'bestehen' und sind einfacher zu behandeln.
 */

/* 
 * Als erstes wird der Darstellungsdienst umgebaut. 
 * 
 * (1) Eigentumsbeschränkung verweist auf Darstellungsdienst und Legendeneintrag. Aus
 * diesem Grund kann nicht zuerst die Eigenstumsbeschränkung umgebaut werden.
 * 
 * (2) Es werden die Themen gemäss 'thema_thema' resp. gemäss Subquery verwendet. Falls
 * es zu einem diesem Thema keine Eigentumsbeschränkungen gibt, müssen die Darstellungsdienste
 * ganz am Schluss (mühsam) gelöscht werden. Sieh Code-Snippets am Ende der Datei.
 * 
 * (3) Eventuell muss der Filter in der Theman-Subquery verfeinert werden, wenn mehr
 * Themen im Themen-XTF/-Dataset vorhanden sind.
 */

WITH darstellungsdienst AS 
(
    SELECT
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS t_id,
        basket_dataset.basket_t_id AS t_basket,
        basket_dataset.datasetname AS t_datasetname,
        CASE 
            WHEN subcode IS NULL THEN acode 
            ELSE subcode
        END AS wmslayer 
    FROM
        (
            SELECT
                acode,
                subcode
            FROM 
                arp_npl_oereb.thema_thema
            WHERE
                subcode IS NOT NULL
            OR 
                acode = 'ch.Waldabstandslinien'
            OR 
                acode = 'ch.Laermemfindlichkeitsstufen'
        ) AS themes
        LEFT JOIN 
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        ON 1=1
)
,
darstellungsdienst_insert AS 
(
    INSERT INTO 
        arp_npl_oereb.transferstruktur_darstellungsdienst 
        (
            t_id,
            t_basket,
            t_datasetname            
        )         
    SELECT 
        t_id,
        t_basket,
        t_datasetname
    FROM 
        darstellungsdienst
)
,
darstellungsdienst_multilingualuri AS 
(
    INSERT INTO
        arp_npl_oereb.multilingualuri 
        (
            t_basket,
            t_datasetname,
            t_seq, 
            transfrstrkstllngsdnst_verweiswms
        )
    SELECT 
        t_basket,
        t_datasetname,
        0,
        t_id

    FROM 
        darstellungsdienst
    RETURNING *
)
INSERT INTO 
    arp_npl_oereb.localiseduri 
    (
        t_basket,
        t_datasetname,
        t_seq,
        alanguage,
        atext,
        multilingualuri_localisedtext
    )
SELECT
    darstellungsdienst.t_basket,
    darstellungsdienst.t_datasetname,
    0,
    'de',
    '${wmsHost}/wms/oereb?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image%2Fpng&TRANSPARENT=true&LAYERS='||wmslayer||'&STYLES=&SRS=EPSG%3A2056&CRS=EPSG%3A2056&DPI=96&WIDTH=1200&HEIGHT=1146&BBOX=2591250%2C1211350%2C2646050%2C1263700',
    darstellungsdienst_multilingualuri.t_id
FROM
    darstellungsdienst
    LEFT JOIN darstellungsdienst_multilingualuri
    ON darstellungsdienst.t_id = darstellungsdienst_multilingualuri.transfrstrkstllngsdnst_verweiswms
;

/* 
 * Eigentumsbeschränkungen und Legendeneinträge
 * 
 * (1) Müssen in einem CTE-Block gemeinsam abgehandlet werden, weil die Eigentumsbeschränkungen
 * auf die Legendeneinträge verweisen aber man die Legendeinträge nur erstellen kann, wenn man
 * weiss welche Eigentumsbeschränkungen es gibt. Benötigt werden ebenfalls noch die Darstellungsdienste
 * aus dem ersten CTE-Block damit man den Legendeneintrag via Thema dem richtigen Darstellungsdient
 * zuweisen kann.
 * 
 * (2) 'typ_grundnutzung IN' (etc.) filtern Eigentumsbeschränkungen weg, die mit keinem Dokument verknüpft sind.
 * Sind solche Objekte vorhanden, handelt es sich in der Regel um einen Datenfehler in den Ursprungsdaten.
 * 'publiziertab IS NOT NULL' filtert Objekte raus, die kein Publikationsdatum haben (Mandatory im Rahmenmodell.)
 *
 * (3) rechtsstatus = 'inKraft': Die Bedingung hier reicht nicht, damit (später) auch nur die Geometrien verwendet
 * werden, die 'inKraft' sind. Grund dafür ist, dass es nicht-'inKraft' Geometrien geben kann, die auf einen
 * Typ zeigen, dem Geometrien zugewiesen sind, die 'inKraft' sind. Nur solche Typen, dem gar keine 'inKraft'
 * Geometrien zugewiesen sind, werden hier rausgefiltert.
 *
 * (4) Ebenfalls reicht die Bedingung 'inKraft' bei den Dokumenten nicht. Hier werden nur Typen rausgefiltert, die 
 * nur Dokumente angehängt haben, die NICHT inKraft sind. Sind bei einem Typ aber sowohl inKraft wie auch nicht-
 * inKraft-Dokumente angehängt, wird korrekterweise der Typ trotzdem verwendet. Bei den Dokumenten muss der
 * Filter nochmals gesetzt werden.
 * 
 * (5) Die richtigen Symbole werden erst nachträglich mit einem andere Gretl-Task korrekt abgefüllt. Hier wird
 * ein Dummy-Symbol gespeichert.
 *
 * (6) Die Query für Waldabstandslinien und Baulinien ist identisch bis auf den Where-Filter. TODO: Vereinfachung 
 * möglich? 
 */

WITH darstellungsdienst AS 
(
    SELECT 
        localiseduri.atext,
        darstellungsdienst.t_id 
    FROM 
        arp_npl_oereb.transferstruktur_darstellungsdienst AS darstellungsdienst
        LEFT JOIN arp_npl_oereb.multilingualuri AS multilingualuri  
        ON multilingualuri.transfrstrkstllngsdnst_verweiswms = darstellungsdienst.t_id 
        LEFT JOIN arp_npl_oereb.localiseduri AS localiseduri 
        ON localiseduri.multilingualuri_localisedtext = multilingualuri.t_id 
)
,
eigentumsbeschraenkung_legendeneintrag AS 
(
    -- Grundnutzung
    SELECT
        DISTINCT ON (typ_grundnutzung.t_ili_tid)
        typ_grundnutzung.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.NutzungsplanungGrundnutzung' AS subthema,
        grundnutzung.rechtsstatus,
        grundnutzung.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbolflaeche,
        CAST(NULL AS bytea) as symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_grundnutzung.bezeichnung AS legendetext_de,
        typ_grundnutzung.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Grundnutzung.'||typ_grundnutzung.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_grundnutzung AS typ_grundnutzung
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_grundnutzung.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_grundnutzung AS grundnutzung
        ON typ_grundnutzung.t_id = grundnutzung.typ_grundnutzung,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.NutzungsplanungGrundnutzung%'
    WHERE
        typ_kt NOT IN 
        (
            'N180_Verkehrszone_Strasse',
            'N181_Verkehrszone_Bahnareal',
            'N182_Verkehrszone_Flugplatzareal',
            'N189_weitere_Verkehrszonen',
            'N210_Landwirtschaftszone',
            'N320_Gewaesser',
            'N329_weitere_Zonen_fuer_Gewaesser_und_ihre_Ufer',
            'N420_Verkehrsflaeche_Strasse', 
            'N421_Verkehrsflaeche_Bahnareal', 
            'N422_Verkehrsflaeche_Flugplatzareal', 
            'N429_weitere_Verkehrsflaechen', 
            'N430_Reservezone_Wohnzone_Mischzone_Kernzone_Zentrumszone',
            'N431_Reservezone_Arbeiten',
            'N432_Reservezone_OeBA',
            'N439_Reservezone',
            'N440_Wald'
        )
        AND
        typ_grundnutzung.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_grundnutzung) 
                typ_grundnutzung
            FROM
                arp_npl.nutzungsplanung_typ_grundnutzung_dokument AS typ_grundnutzung_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_grundnutzung_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        grundnutzung.publiziertab IS NOT NULL
        AND
        grundnutzung.rechtsstatus = 'inKraft'
    
    UNION ALL 
    
    -- Überlagernd (Flaeche) 
    SELECT
        DISTINCT ON (typ_ueberlagernd_flaeche.t_ili_tid)
        typ_ueberlagernd_flaeche.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.NutzungsplanungUeberlagernd' AS subthema,
        ueberlagernd_flaeche.rechtsstatus,
        ueberlagernd_flaeche.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbolflaeche,
        CAST(NULL AS bytea) as symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_ueberlagernd_flaeche.bezeichnung AS legendetext_de,
        typ_ueberlagernd_flaeche.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Ueberlagernd_Flaeche.'||typ_ueberlagernd_flaeche.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche AS typ_ueberlagernd_flaeche
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_ueberlagernd_flaeche.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_ueberlagernd_flaeche AS ueberlagernd_flaeche
        ON typ_ueberlagernd_flaeche.t_id = ueberlagernd_flaeche.typ_ueberlagernd_flaeche,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.NutzungsplanungUeberlagernd%'
    WHERE
        (
            typ_kt IN 
            (
                'N510_ueberlagernde_Ortsbildschutzzone',
                'N523_Landschaftsschutzzone',
                'N526_kantonale_Landwirtschafts_und_Schutzzone_Witi',
                'N527_kantonale_Uferschutzzone',
                'N528_kommunale_Uferschutzzone_ausserhalb_Bauzonen',
                'N529_weitere_Schutzzonen_fuer_Lebensraeume_und_Landschaften',
                'N590_Hofstattzone_Freihaltezone',
                'N591_Bauliche_Einschraenkungen',
                'N690_kantonales_Vorranggebiet_Natur_und_Landschaft',
                'N691_kommunales_Vorranggebiet_Natur_und_Landschaft',
                'N692_Planungszone',
                'N699_weitere_flaechenbezogene_Festlegungen_NP',
                'N812_geologisches_Objekt',
                'N813_Naturobjekt',
                'N822_schuetzenswertes_Kulturobjekt',
                'N823_erhaltenswertes_Kulturobjekt'
            )
            OR
            (
                typ_kt = 'N599_weitere_ueberlagernde_Nutzungszonen' AND verbindlichkeit = 'Nutzungsplanfestlegung'
            ) 
        )   
        AND
        typ_ueberlagernd_flaeche.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_ueberlagernd_flaeche) 
                typ_ueberlagernd_flaeche
            FROM
                arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche_dokument AS typ_ueberlagernd_flaeche_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_ueberlagernd_flaeche_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        ueberlagernd_flaeche.publiziertab IS NOT NULL
        AND
        ueberlagernd_flaeche.rechtsstatus = 'inKraft'
        
    UNION ALL
    
    -- Überlagernd (Linie) 
    SELECT
        DISTINCT ON (typ_ueberlagernd_linie.t_ili_tid)
        typ_ueberlagernd_linie.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.NutzungsplanungUeberlagernd' AS subthema,
        ueberlagernd_linie.rechtsstatus,
        ueberlagernd_linie.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        CAST(NULL AS bytea) as symbolflaeche,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_ueberlagernd_linie.bezeichnung AS legendetext_de,
        typ_ueberlagernd_linie.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Ueberlagernd_Linie.'||typ_ueberlagernd_linie.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_ueberlagernd_linie AS typ_ueberlagernd_linie
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_ueberlagernd_linie.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_ueberlagernd_linie AS ueberlagernd_linie
        ON typ_ueberlagernd_linie.t_id = ueberlagernd_linie.typ_ueberlagernd_linie,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.NutzungsplanungUeberlagernd%'
    WHERE
        (
            typ_kt = 'N799_weitere_linienbezogene_Festlegungen_NP' AND verbindlichkeit = 'Nutzungsplanfestlegung'
        )
        AND
        typ_ueberlagernd_linie.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_ueberlagernd_linie) 
                typ_ueberlagernd_linie
            FROM
                arp_npl.nutzungsplanung_typ_ueberlagernd_linie_dokument AS typ_ueberlagernd_linie_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_ueberlagernd_linie_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        ueberlagernd_linie.publiziertab IS NOT NULL
        AND
        ueberlagernd_linie.rechtsstatus = 'inKraft'

    UNION ALL

    -- Überlagernd (Punkt) 
    SELECT
        DISTINCT ON (typ_ueberlagernd_punkt.t_ili_tid)
        typ_ueberlagernd_punkt.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.NutzungsplanungUeberlagernd' AS subthema,
        ueberlagernd_punkt.rechtsstatus,
        ueberlagernd_punkt.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        CAST(NULL AS bytea) as symbolflaeche,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_ueberlagernd_punkt.bezeichnung AS legendetext_de,
        typ_ueberlagernd_punkt.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Ueberlagernd_punkt.'||typ_ueberlagernd_punkt.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_ueberlagernd_punkt AS typ_ueberlagernd_punkt
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_ueberlagernd_punkt.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_ueberlagernd_punkt AS ueberlagernd_punkt
        ON typ_ueberlagernd_punkt.t_id = ueberlagernd_punkt.typ_ueberlagernd_punkt,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.NutzungsplanungUeberlagernd%'
    WHERE
        (
            typ_kt IN 
            (
                'N811_erhaltenswerter_Einzelbaum',
                'N822_schuetzenswertes_Kulturobjekt',
                'N823_erhaltenswertes_Kulturobjekt'
            )
            OR
            (
                typ_kt = 'N899_weitere_punktbezogene_Festlegungen_NP' AND verbindlichkeit = 'Nutzungsplanfestlegung'
            )   
        )
        AND
        typ_ueberlagernd_punkt.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_ueberlagernd_punkt) 
                typ_ueberlagernd_punkt
            FROM
                arp_npl.nutzungsplanung_typ_ueberlagernd_punkt_dokument AS typ_ueberlagernd_punkt_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_ueberlagernd_punkt_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        ueberlagernd_punkt.publiziertab IS NOT NULL
        AND
        ueberlagernd_punkt.rechtsstatus = 'inKraft'

    UNION ALL 

    -- Sondernutzungspläne
    SELECT
        DISTINCT ON (typ_ueberlagernd_flaeche.t_ili_tid)
        typ_ueberlagernd_flaeche.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.NutzungsplanungUeberlagernd' AS subthema,
        ueberlagernd_flaeche.rechtsstatus,
        ueberlagernd_flaeche.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbolflaeche,
        CAST(NULL AS bytea) as symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_ueberlagernd_flaeche.bezeichnung AS legendetext_de,
        typ_ueberlagernd_flaeche.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Ueberlagernd_Flaeche.'||typ_ueberlagernd_flaeche.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche AS typ_ueberlagernd_flaeche
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_ueberlagernd_flaeche.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_ueberlagernd_flaeche AS ueberlagernd_flaeche
        ON typ_ueberlagernd_flaeche.t_id = ueberlagernd_flaeche.typ_ueberlagernd_flaeche,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.NutzungsplanungSondernutzungsplaene%'
    WHERE
        typ_kt IN 
        (
            'N610_Perimeter_kantonaler_Nutzungsplan',
            'N611_Perimeter_kommunaler_Gestaltungsplan',
            'N620_Perimeter_Gestaltungsplanpflicht'
        ) 
        AND
        typ_ueberlagernd_flaeche.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_ueberlagernd_flaeche) 
                typ_ueberlagernd_flaeche
            FROM
                arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche_dokument AS typ_ueberlagernd_flaeche_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_ueberlagernd_flaeche_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        ueberlagernd_flaeche.publiziertab IS NOT NULL
        AND
        ueberlagernd_flaeche.rechtsstatus = 'inKraft'
        
    UNION ALL

    -- Baulinien
    SELECT
        DISTINCT ON (typ_erschliessung_linienobjekt.t_ili_tid)
        typ_erschliessung_linienobjekt.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Nutzungsplanung' AS thema,
        'ch.SO.Baulinien' AS subthema,
        erschliessung_linienobjekt.rechtsstatus,
        erschliessung_linienobjekt.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        CAST(NULL AS bytea) as symbolflaeche,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_erschliessung_linienobjekt.bezeichnung AS legendetext_de,
        typ_erschliessung_linienobjekt.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Erschliessung_Linienobjekt.'||typ_erschliessung_linienobjekt.t_datasetname AS artcodeliste
    FROM
        arp_npl.erschlssngsplnung_typ_erschliessung_linienobjekt AS typ_erschliessung_linienobjekt
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_erschliessung_linienobjekt.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.erschlssngsplnung_erschliessung_linienobjekt AS erschliessung_linienobjekt
        ON typ_erschliessung_linienobjekt.t_id = erschliessung_linienobjekt.typ_erschliessung_linienobjekt,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.SO.Baulinien%'
    WHERE
        typ_kt IN 
        (
            'E711_Baulinie_Strasse_kantonal',
            'E712_Vorbaulinie_kantonal',
            'E713_Gestaltungsbaulinie_kantonal',
            'E714_Rueckwaertige_Baulinie_kantonal',
            'E715_Baulinie_Infrastruktur_kantonal',
            'E719_weitere_nationale_und_kantonale_Baulinien',
            'E720_Baulinie_Strasse',
            'E721_Vorbaulinie',
            'E722_Gestaltungsbaulinie',
            'E723_Rueckwaertige_Baulinie',
            'E724_Baulinie_Infrastruktur',
            'E726_Baulinie_Hecke',
            'E727_Baulinie_Gewaesser',
            'E728_Immissionsstreifen',
            'E729_weitere_kommunale_Baulinien'
        )
        AND
        typ_erschliessung_linienobjekt.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_erschliessung_linienobjekt) 
                typ_erschliessung_linienobjekt
            FROM
                arp_npl.erschlssngsplnung_typ_erschliessung_linienobjekt_dokument AS typ_erschliessung_linienobjekt_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_erschliessung_linienobjekt_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        erschliessung_linienobjekt.publiziertab IS NOT NULL
        AND
        erschliessung_linienobjekt.rechtsstatus = 'inKraft'

    UNION ALL

    -- Waldabstandslinie 
    SELECT
        DISTINCT ON (typ_erschliessung_linienobjekt.t_ili_tid)
        typ_erschliessung_linienobjekt.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Waldabstandslinien' AS thema,
        CAST(NULL AS text) AS subthema,
        erschliessung_linienobjekt.rechtsstatus,
        erschliessung_linienobjekt.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        CAST(NULL AS bytea) as symbolflaeche,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_erschliessung_linienobjekt.bezeichnung AS legendetext_de,
        typ_erschliessung_linienobjekt.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Erschliessung_Linienobjekt.'||typ_erschliessung_linienobjekt.t_datasetname AS artcodeliste
    FROM
        arp_npl.erschlssngsplnung_typ_erschliessung_linienobjekt AS typ_erschliessung_linienobjekt
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_erschliessung_linienobjekt.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.erschlssngsplnung_erschliessung_linienobjekt AS erschliessung_linienobjekt
        ON typ_erschliessung_linienobjekt.t_id = erschliessung_linienobjekt.typ_erschliessung_linienobjekt,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.Waldabstandslinien%'
    WHERE
        typ_kt IN 
        (
            'E725_Waldabstandslinie'
        )
        AND
        typ_erschliessung_linienobjekt.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_erschliessung_linienobjekt) 
                typ_erschliessung_linienobjekt
            FROM
                arp_npl.erschlssngsplnung_typ_erschliessung_linienobjekt_dokument AS typ_erschliessung_linienobjekt_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_erschliessung_linienobjekt_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        erschliessung_linienobjekt.publiziertab IS NOT NULL
        AND
        erschliessung_linienobjekt.rechtsstatus = 'inKraft'

    UNION ALL

    -- Laermempfindlichkeit 
    SELECT
        DISTINCT ON (typ_ueberlagernd_flaeche.t_ili_tid)
        typ_ueberlagernd_flaeche.t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        'ch.Laermemfindlichkeitsstufen' AS thema,
        CAST(NULL AS text) AS subthema,
        ueberlagernd_flaeche.rechtsstatus,
        ueberlagernd_flaeche.publiziertab,
        darstellungsdienst.t_id AS darstellungsdienst,
        amt.t_id AS zustaendigestelle,
        -- legendeneintrag Attribute
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS legendeneintrag_t_id,
        decode('iVBORw0KGgoAAAANSUhEUgAAAEYAAAAjCAYAAAApF3xtAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAD8AAAA/AB2OVKxAAAABl0RVh0U29mdHdhcmUAd3d3Lmlua3NjYXBlLm9yZ5vuPBoAAAOoSURBVGiB7ZpfaM1hGMc/PzvGjFLDzGwixVD+xOnsQmOrlcTGldquFq2RzJ+yG5KUpYw0t65WxtXcUGwXXJhIys1xQZSNkWSOzUp6XDzO3vM6e4/fjB3yfuvtfd7v+zzv+zvfnvfP+Z0TCAgeaZiS7Qf4W+GFcSBitYqKIBbL0qNkGffuwevXpi0go6WmRv5b1NRIqhZ+KTnghXHAC+OAF8YBL4wDXhgH3MK8fAnr19tlwwbYtg3a2+HzZ+P76JHxOXvWHqe2VvnNmw0XiylXWQmJhO1//rwZ6+pV5To6DHfxou3/4QNs2qR9Gzcqd+uW8T9xwvY/dsz0dXe7lXHeY+Jx61xPK2VlIm/fqm9Pj+H37bPvB4sWKZ+fb7ggMP4tLYZ//lwkL8/0tbUp39pquPx8kb4+E9PcbPqmTVNuaEikoEC5mTNFEgnlBwc1HkTmzhUZHp7gPWbFCmhthePHYdky5eJxaGkJFZ4R587Bs2dqHzliZ+JYGBoy88bj6RkEMGMGNDaq/ekTXLmidmenxgM0NUFennueUBmzc6fhX70SiUSULymZeMaASG2tPUamjAGN7+0Vqa62+WTGiIj094vk5ipfXq5cNKrt6dNF3ryxn3PCN9+iIi0A79+PO9xCEGjd1QV1dTaXKUYEduyAmzfdMQsWwK5davf2wuXLcP++tuvrYd68jNP82qk05TcdZsuX6wYMMDCg4yaXgAtNTSrEwIC2q6thyZKxfQ8eNPbu3VoHgc07kP3juq0NcnLUbmiAdesy+0ejJrsikfRTMBVr1hjhh4e13rJF98yfIPvCrF4Ne/dCaSmcOhUu5vRpKC6Gw4dh1arMvj9mx6FDoaaI/NxlEnDhgpawWLgQ+vrC+W7dCrNm6X1pzhyoqgoV9msZkzzykntNbq7p+/LF9k22U30mE0GgSw5g6tTQYeMX5to1ePdO7dJSrRcvNifDnTswMqL248fmrdjSpeOeKpsIt5R6evQKnUjA06eGT+70xcW6qV2/Dk+eQFkZrFwJt2/r0QqwZ89vfvQ/i3DCDA7Cw4emnZOjG9+BA4a7dEm/Rz14AC9eaAHNpP37jYj/CNzCzJ+vXwNSEQTKV1VplqSisBDu3oUbNzRTPn5Un+3bYe1a2/fMGfj6dexLVixm5q2o0Lqy0nDRaHrM0aN62XTtYydP6r44e7bz46bBvwz/Dv8yPBy8MA54YRzwwjjghXEgsP4G4n+7Hm3awniMwi8lB7wwDnwD/bFRBvNxDWsAAAAASUVORK5CYII=', 'base64') AS symbolflaeche,
        CAST(NULL AS bytea) as symbollinie,
        CAST(NULL AS bytea) as symbolpunkt,
        typ_ueberlagernd_flaeche.bezeichnung AS legendetext_de,
        typ_ueberlagernd_flaeche.code_kommunal AS artcode,
        'urn:fdc:ilismeta.interlis.ch:2017:NP_Typ_Kanton_Ueberlagernd_Flaeche.'||typ_ueberlagernd_flaeche.t_datasetname AS artcodeliste
    FROM
        arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche AS typ_ueberlagernd_flaeche
        LEFT JOIN arp_npl_oereb.amt_amt AS amt
        ON typ_ueberlagernd_flaeche.t_datasetname = RIGHT(amt.t_ili_tid, 4)
        LEFT JOIN arp_npl.nutzungsplanung_ueberlagernd_flaeche AS ueberlagernd_flaeche
        ON typ_ueberlagernd_flaeche.t_id = ueberlagernd_flaeche.typ_ueberlagernd_flaeche,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
        LEFT JOIN darstellungsdienst
        ON darstellungsdienst.atext ILIKE '%ch.Laermemfindlichkeitsstufen%'
    WHERE
        (
            typ_kt IN 
            (
                'N680_Empfindlichkeitsstufe_I',
                'N681_Empfindlichkeitsstufe_II',
                'N682_Empfindlichkeitsstufe_II_aufgestuft',
                'N683_Empfindlichkeitsstufe_III',
                'N684_Empfindlichkeitsstufe_III_aufgestuft',
                'N685_Empfindlichkeitsstufe_IV',
                'N686_keine_Empfindlichkeitsstufe'
            )
            OR
            (
                typ_kt = 'N599_weitere_ueberlagernde_Nutzungszonen' AND verbindlichkeit = 'Nutzungsplanfestlegung'
            ) 
        )   
        AND
        typ_ueberlagernd_flaeche.t_id IN 
        (
            SELECT
                DISTINCT ON (typ_ueberlagernd_flaeche) 
                typ_ueberlagernd_flaeche
            FROM
                arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche_dokument AS typ_ueberlagernd_flaeche_dokument
                LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
                ON dokument.t_id = typ_ueberlagernd_flaeche_dokument.dokument
            WHERE
                dokument.rechtsstatus = 'inKraft'        
        )  
        AND
        ueberlagernd_flaeche.publiziertab IS NOT NULL
        AND
        ueberlagernd_flaeche.rechtsstatus = 'inKraft'
)
,
legendeneintrag_insert AS 
(
    INSERT INTO
        arp_npl_oereb.transferstruktur_legendeeintrag 
        (
            t_id,
            t_basket,
            t_datasetname,
            symbolflaeche,
            symbollinie,
            symbolpunkt,
            legendetext_de,
            artcode,
            artcodeliste,
            thema,
            subthema,
            darstellungsdienst
        )
    SELECT 
        legendeneintrag_t_id,
        basket_t_id,
        datasetname,
        symbolflaeche,
        symbollinie,
        symbolpunkt,
        legendetext_de,
        artcode,
        artcodeliste,
        thema,
        subthema,
        darstellungsdienst
    FROM 
       eigentumsbeschraenkung_legendeneintrag 
)  
INSERT INTO 
    arp_npl_oereb.transferstruktur_eigentumsbeschraenkung 
    (
        t_id,
        t_basket,
        t_datasetname,
        thema,
        subthema,
        rechtsstatus,
        publiziertab,
        darstellungsdienst,
        legende,
        zustaendigestelle
    )
    SELECT
        t_id,
        basket_t_id,
        datasetname,
        thema,
        subthema,
        rechtsstatus,
        publiziertab,
        darstellungsdienst,
        legendeneintrag_t_id,
        zustaendigestelle
    FROM 
        eigentumsbeschraenkung_legendeneintrag
;

/*
 * Update (Korrektur) der zuständigen Stellen.
 * 
 * Die zuständige Stelle einiger Typen ist nicht die Gemeinde, sondern ein
 * kantonales Amt (ARP oder AVT). Der Einfachheithalber wird zuerst alles
 * der Gemeinde zugewissen (obere Query). Mit einem Update werden den 
 * einzelnen Typen die korrekte zuständige Stelle zugewiesen.
 */

WITH eigentumsbeschraenkung_legendeneintrag AS 
(
    SELECT 
        eigentumsbeschraenkung.t_id
    FROM 
        arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
        LEFT JOIN arp_npl_oereb.transferstruktur_legendeeintrag AS legendeeintrag
        ON legendeeintrag.t_id = eigentumsbeschraenkung.legende 
    WHERE 
        substring(legendeeintrag.artcode, 1, 3) IN ('526', '527', '610', '690')
)
UPDATE 
    arp_npl_oereb.transferstruktur_eigentumsbeschraenkung
SET
    zustaendigestelle = subquery.t_id
FROM
(
    SELECT 
        t_id 
    FROM 
        arp_npl_oereb.amt_amt
    WHERE
        t_ili_tid = 'ch.so.arp'
) AS subquery
WHERE
    transferstruktur_eigentumsbeschraenkung.t_id IN (SELECT t_id FROM eigentumsbeschraenkung_legendeneintrag)
;

WITH eigentumsbeschraenkung_legendeneintrag AS 
(
    SELECT 
        eigentumsbeschraenkung.t_id
    FROM 
        arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
        LEFT JOIN arp_npl_oereb.transferstruktur_legendeeintrag AS legendeeintrag
        ON legendeeintrag.t_id = eigentumsbeschraenkung.legende 
    WHERE 
        substring(legendeeintrag.artcode, 1, 3) IN ('711', '712', '713', '714', '715', '719')
)
UPDATE 
    arp_npl_oereb.transferstruktur_eigentumsbeschraenkung
SET
    zustaendigestelle = subquery.t_id
FROM
(
    SELECT 
        t_id 
    FROM 
        arp_npl_oereb.amt_amt
    WHERE
        t_ili_tid = 'ch.so.avt'
) AS subquery
WHERE
    transferstruktur_eigentumsbeschraenkung.t_id IN (SELECT t_id FROM eigentumsbeschraenkung_legendeneintrag)
;

/*
 * Es werden die Dokumente der ersten Hierarchie-Ebene ("direkt verlinkt") abgehandelt, d.h.
 * "HinweisWeitere"-Dokumente werden in einem weiteren Schritt bearbeitet. Um die Dokumente
 * zu kopieren, muss auch die n-m-Zwischentabelle bearbeitet werden, wegen der
 * Foreign Keys Constraints. Bemerkungen:
 * 
 * (1)  In den Ausgangsdaten müssen die Attribute Abkuerzung und Rechtsvorschrift zwingend 
 * gesetzt sein, sonst kann nicht korrekt umgebaut werden. 
 * 
 * (2) Relativ mühsam ist der Umstand, dass bereits Daten in der Dokumenten-
 * Tabelle vorhanden sind (die kantonalen Gesetze). Deren Primary Keys hat
 * man nicht im Griff und so kann es vorkommen, dass es zu einer Kollision
 * mit den zu kopierenden Daten kommt. Abhilfe schafft beim Erstellen des
 * Staging-Schemas der Parameter --idSeqMin. Damit kann der Startwert der
 * Sequenz gesetzt werden, um solche Kollisionen mit grösster Wahrscheinlichkeit
 * zu verhindern.
 * 
 * (3) Die t_ili_tid kann nicht einfach so aus der Quelltabelle übernommen werden,
 * da sie keine valide OID ist (die gemäss Modell verlangt wird). Gemäss Kommentar
 * sollte sie zudem wie eine Domain aufgebaut sein. Der Einfachheit halber (Referenzen
 * gibt es ja in der DB darauf nicht, sondern auf den PK) mache ich aus der UUID eine
 * valide OID mittels Substring, Replace und Concat.
 * 
 * (4) Es gibt Objekte (Typen), die in den Kataster aufgenommen werden müssen (gemäss
 * Excelliste) aber keine Dokumente zugewiesen haben. -> Datenfehler. Aus diesem Grund
 * wird eine Where-Clause verwendet (dokument.t_id IS NOT NULL). 
 * 2019-08-03 / sz: Wieder entfernt, da man diese Daten bereits ganz zu Beginn (erste 
 * Query) rausfiltern muss.
 * 
 * (5) In der 'hinweisvorschrift'-Query werden nur diejenigen Dokumente verwendet, die
 * inKraft sind. Durch den RIGHT JOIN in der Query 'vorschriften_dokument' werden
 * dann ebenfalls nur die Dokumente selektiert, die inKraft sind. Ein weiterer Filter
 * ist hier unnötig.
 */

WITH basket_dataset AS 
(
    SELECT
        basket.t_id AS basket_t_id,
        dataset.datasetname AS datasetname               
    FROM
        arp_npl_oereb.t_ili2db_dataset AS dataset
        LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
        ON basket.dataset = dataset.t_id
    WHERE
        dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
)
,
hinweisvorschrift AS 
(
    SELECT
        t_typ_dokument.t_id,
        basket_dataset.basket_t_id AS t_basket,
        basket_dataset.datasetname AS t_datasetname,        
        t_typ_dokument.eigentumsbeschraenkung,
        t_typ_dokument.vorschrift_vorschriften_dokument
    FROM
    (
        -- Grundnutzung
        SELECT
            typ_dokument.t_id,
            typ_dokument.typ_grundnutzung AS eigentumsbeschraenkung,
            typ_dokument.dokument AS vorschrift_vorschriften_dokument
        FROM
            arp_npl.nutzungsplanung_typ_grundnutzung_dokument AS typ_dokument
            LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
            ON dokument.t_id = typ_dokument.dokument
        WHERE
            dokument.rechtsstatus = 'inKraft'
            
        UNION ALL
        
        -- Überlagernd (Fläche) + Sondernutzungspläne + Lärmempfindlichkeitsstufen 
        SELECT
            typ_dokument.t_id,
            typ_dokument.typ_ueberlagernd_flaeche AS eigentumsbeschraenkung,
            typ_dokument.dokument AS vorschrift_vorschriften_dokument
        FROM
            arp_npl.nutzungsplanung_typ_ueberlagernd_flaeche_dokument AS typ_dokument
            LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
            ON dokument.t_id = typ_dokument.dokument
        WHERE
            dokument.rechtsstatus = 'inKraft'

        UNION ALL
        
        -- Überlagernd (Linie)        
        SELECT
            typ_dokument.t_id,
            typ_dokument.typ_ueberlagernd_linie AS eigentumsbeschraenkung,
            typ_dokument.dokument AS vorschrift_vorschriften_dokument
        FROM
            arp_npl.nutzungsplanung_typ_ueberlagernd_linie_dokument AS typ_dokument
            LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
            ON dokument.t_id = typ_dokument.dokument
        WHERE
            dokument.rechtsstatus = 'inKraft'

        UNION ALL

        -- Überlagernd (Punkt)        
        SELECT
            typ_dokument.t_id,
            typ_dokument.typ_ueberlagernd_punkt AS eigentumsbeschraenkung,
            typ_dokument.dokument AS vorschrift_vorschriften_dokument
        FROM
            arp_npl.nutzungsplanung_typ_ueberlagernd_punkt_dokument AS typ_dokument
            LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
            ON dokument.t_id = typ_dokument.dokument
        WHERE
            dokument.rechtsstatus = 'inKraft'

        UNION ALL

        -- Baulinien + Waldabstandslinien
        SELECT
            typ_dokument.t_id,
            typ_dokument.typ_erschliessung_linienobjekt AS eigentumsbeschraenkung,
            typ_dokument.dokument AS vorschrift_vorschriften_dokument
        FROM
            arp_npl.erschlssngsplnung_typ_erschliessung_linienobjekt_dokument AS typ_dokument
            LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
            ON dokument.t_id = typ_dokument.dokument
        WHERE
            dokument.rechtsstatus = 'inKraft'
    ) AS t_typ_dokument
    RIGHT JOIN arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
    ON t_typ_dokument.eigentumsbeschraenkung = eigentumsbeschraenkung.t_id,
    basket_dataset
)
,
vorschriften_dokument AS
(
    INSERT INTO 
        arp_npl_oereb.vorschriften_dokument
        (
            t_id,
            t_basket,
            t_datasetname,
            t_ili_tid,
            typ,            
            titel_de,
            abkuerzung_de,
            offiziellenr,
            kanton,
            gemeinde,
            rechtsstatus,
            publiziertab,
            zustaendigestelle
        )   
    SELECT 
        DISTINCT ON (dokument.t_id)
        dokument.t_id AS t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        '_'||SUBSTRING(REPLACE(CAST(dokument.t_ili_tid AS text), '-', ''),1,15) AS t_ili_tid,
        CASE
            WHEN rechtsvorschrift IS FALSE
                THEN 'Hinweis'
            ELSE 'Rechtsvorschrift'
        END AS typ,
        COALESCE(dokument.titel || ' - ' || dokument.offiziellertitel, dokument.titel) AS titel_de,
        dokument.abkuerzung AS abkuerzung_de,
        dokument.offiziellenr AS offiziellenr,
        dokument.kanton AS kanton,
        dokument.gemeinde AS gemeinde,
        dokument.rechtsstatus AS rechtsstatus,
        dokument.publiziertab AS publiziertab,
        CASE
            WHEN abkuerzung = 'RRB'
                THEN 
                (
                    SELECT 
                        t_id
                    FROM
                        arp_npl_oereb.amt_amt
                    WHERE
                        t_datasetname = 'ch.so.agi.zustaendigestellen.oereb'
                    AND
                        t_ili_tid = 'ch.so.sk'
                )
            ELSE
                (
                    SELECT 
                        t_id
                    FROM
                        arp_npl_oereb.amt_amt
                    WHERE
                        RIGHT(t_ili_tid, 4) = CAST(gemeinde AS TEXT)
                )
         END AS zustaendigestelle
    FROM
        arp_npl.rechtsvorschrften_dokument AS dokument
        RIGHT JOIN hinweisvorschrift
        ON dokument.t_id = hinweisvorschrift.vorschrift_vorschriften_dokument,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset   
   RETURNING *
)
INSERT INTO
    arp_npl_oereb.transferstruktur_hinweisvorschrift
    (
        t_id,
        t_basket,
        t_datasetname,
        eigentumsbeschraenkung,
        vorschrift  
    )
SELECT
    t_id, -- TODO: muss nicht zwingend Original-TID sein, oder?
    t_basket,
    t_datasetname,
    eigentumsbeschraenkung,
    vorschrift_vorschriften_dokument
FROM
    hinweisvorschrift
;

/*
 * Umbau der zusätzlichen Dokumente, die im Originalmodell in der 
 * HinweisWeitereDokumente vorkommen und nicht direkt (via Zwischen-
 * Tabelle) mit der Eigentumsbeschränkung / mit dem Typ verknüpft sind. 
 * 
 * (1) Flachwalzen: Anstelle der gnietigen HinweisWeitereDokumente-Tabelle 
 * kann man alles flachwalzen, d.h. alle Dokument-zu-Dokument-Links werden 
 * direkt mit an den Typ / an die Eigentumsbeschränkung verlinkt. Dazu muss man 
 * für jedes Dokument in dieser Schleife das Top-Level-Dokument (das 'wirkliche'
 * Ursprungs-Dokument) kennen, damit dann auch noch die Verbindungstabelle
 * (transferstruktur_hinweisvorschrift) zwischen Eigentumsbeschränkung und 
 * Dokument abgefüllt werden kann.
 * 
 * (2) Umbau sehr gut validieren (wegen des Flachwalzens)!
 * 
 * (3) Die rekursive CTE muss am Anfang stehen.
 * 
 * (4) STIMMT DIESE AUSSAGE NOCH? Achtung: Beim Einfügen der zusätzlichen Dokumente in die Dokumententabelle
 * kann es Duplikate geben, da zwei verschiedene Top-Level-Dokumente auf das gleiche
 * weitere Dokument verweisen. Das wirft einen Fehler (Primary Key Constraint). Aus
 * diesem Grund muss beim Inserten noch ein DISTINCT auf die t_id gemacht werden. 
 * Beim anschliessenden Herstellen der Verknüpfung aber nicht mehr.
 * 
 * (5)  Bei 'zusaetzliche_dokumente AS ..' können Dokumente vorkommen, die bereits aufgrund aus einer 
 * direkten Verlinkung in einem anderen Thema/Subthemas in der Dokumenten-Tabelle vorhanden sind. 
 * Diese Duplikaten werden erst beim Inserten gefiltert, da man die trotzdem eine weitere Beziehung
 * in eines anderen Themas/Subthemas stammen. Das Filtern wird erst beim Insert gemacht, da
 * man die Beziehung in 'transferstruktur_hinweisvorschrift' einfügen muss. Sonst geht dieses
 * Wissen verloren. Braucht auch noch einen Filter beim Inserten dieser Beziehung, sonst kommen
 * ebenfalls die bereits direkt verlinkten.
 * 
 * (6) Weil jetzt in der Tabelle 'arp_npl_oereb.vorschriften_dokument' nur noch (vorangehende Query)
 * die inKraft-Dokumente sind, weiss ich nicht genau, was passiert wenn ein nicht-inKraft-
 * Dokument irgendwo zwischen zwei inKraft-Dokumenten bei der Rekursion zu liegen kommt.
 * Ich bin nicht sicher, ob die 'ursprung' und 'hinweis'-Filter im zweiten Teil der
 * rekursiven Query etwas bewirken.
 * Müsste man anhand eines einfachen Beispieles ausprobieren.
 */

WITH RECURSIVE x(ursprung, hinweis, parents, last_ursprung, depth) AS 
(
    SELECT 
        ursprung, 
        hinweis, 
        ARRAY[ursprung] AS parents, 
        ursprung AS last_ursprung, 
        0 AS "depth" 
    FROM 
        arp_npl.rechtsvorschrften_hinweisweiteredokumente
    WHERE
        ursprung != hinweis
    AND ursprung IN 
    (
        SELECT
            t_id
        FROM
            arp_npl_oereb.vorschriften_dokument
        WHERE
            t_datasetname = 'ch.so.arp.nutzungsplanung'
    )

    UNION ALL
  
    SELECT 
        x.ursprung, 
        x.hinweis, 
        parents||t1.hinweis, 
        t1.hinweis AS last_ursprung, 
        x."depth" + 1
    FROM 
        x 
        INNER JOIN arp_npl.rechtsvorschrften_hinweisweiteredokumente t1 
        ON (last_ursprung = t1.ursprung)
    WHERE 
        t1.hinweis IS NOT NULL
    AND x.ursprung IN 
    (
        SELECT
            t_id
        FROM
            arp_npl_oereb.vorschriften_dokument
        WHERE
            t_datasetname = 'ch.so.arp.nutzungsplanung'
    )
),
zusaetzliche_dokumente AS 
(
    SELECT 
        DISTINCT ON (x.last_ursprung, x.ursprung)
        x.ursprung AS top_level_dokument,
        x.last_ursprung AS t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        '_'||SUBSTRING(REPLACE(CAST(dokument.t_ili_tid AS text), '-', ''),1,15) AS t_ili_tid,
        CASE
            WHEN rechtsvorschrift IS FALSE
                THEN 'Hinweis'
            ELSE 'Rechtsvorschrift'
        END AS typ,
        COALESCE(dokument.titel || ' - ' || dokument.offiziellertitel, dokument.titel) AS titel_de,
        dokument.abkuerzung AS abkuerzung_de,
        dokument.offiziellenr AS offiziellenr,
        dokument.kanton AS kanton,
        dokument.gemeinde AS gemeinde,
        dokument.rechtsstatus AS rechtsstatus,
        dokument.publiziertab AS publiziertab,
        CASE
            WHEN abkuerzung = 'RRB'
                THEN 
                (
                    SELECT 
                        t_id
                    FROM
                        arp_npl_oereb.amt_amt
                    WHERE
                        t_datasetname = 'ch.so.agi.zustaendigestellen.oereb' 
                    AND
                        t_ili_tid = 'ch.so.sk' 
                )
            ELSE
                (
                    SELECT 
                        t_id
                    FROM
                        arp_npl_oereb.amt_amt
                    WHERE
                        RIGHT(t_ili_tid, 4) = CAST(gemeinde AS TEXT)
                )
         END AS zustaendigestelle        
    FROM 
        x
        LEFT JOIN arp_npl.rechtsvorschrften_dokument AS dokument
        ON dokument.t_id = x.last_ursprung,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
)
,
zusaetzliche_dokumente_insert AS 
(
    INSERT INTO 
        arp_npl_oereb.vorschriften_dokument
        (
            t_id,
            t_basket,
            t_datasetname,
            t_ili_tid,
            typ,            
            titel_de,
            abkuerzung_de,
            offiziellenr,
            kanton,
            gemeinde,
            rechtsstatus,
            publiziertab,
            zustaendigestelle
        )   
    SELECT
        DISTINCT ON (t_id)    
        t_id,
        basket_t_id,
        datasetname,
        t_ili_tid,
        typ,        
        titel_de,
        abkuerzung_de,
        offiziellenr,
        kanton,
        gemeinde,
        rechtsstatus,
        publiziertab,
        zustaendigestelle
    FROM
        zusaetzliche_dokumente
    WHERE
        t_id NOT IN 
        (
            SELECT
                t_id
            FROM
                arp_npl_oereb.vorschriften_dokument
            WHERE
                t_datasetname = 'ch.so.arp.nutzungsplanung'
        )
)
INSERT INTO 
    arp_npl_oereb.transferstruktur_hinweisvorschrift 
    (
        t_basket,
        t_datasetname,
        eigentumsbeschraenkung,
        vorschrift
    )
    SELECT 
        DISTINCT 
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        hinweisvorschrift.eigentumsbeschraenkung,
        zusaetzliche_dokumente.t_id AS vorschrift_vorschriften_dokument
    FROM 
        zusaetzliche_dokumente
        LEFT JOIN arp_npl_oereb.transferstruktur_hinweisvorschrift AS hinweisvorschrift
        ON hinweisvorschrift.vorschrift = zusaetzliche_dokumente.top_level_dokument,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
    WHERE
        NOT EXISTS 
        (
            SELECT 
                eigentumsbeschraenkung, 
                vorschrift 
            FROM 
                arp_npl_oereb.transferstruktur_hinweisvorschrift     
            WHERE 
                eigentumsbeschraenkung = hinweisvorschrift.eigentumsbeschraenkung
                AND
                vorschrift = zusaetzliche_dokumente.t_id
        )
;

/*
 * Datenumbau der Links auf die Dokumente, die im Rahmenmodell 'multilingual' sind und daher eher
 * mühsam normalisert sind.
 * 
 * (1) Im NPL-Modell sind die URL nicht vollständig, sondern es werden nur Teile des Pfads verwaltet.
 * Beim Datenumbau in das Rahmenmodell wird daraus eine vollständige URL gemacht.
 */

WITH multilingualuri AS
(
    INSERT INTO
        arp_npl_oereb.multilingualuri
        (
            t_id,
            t_basket,
            t_datasetname,
            t_seq,
            vorschriften_dokument_textimweb
        )
    SELECT
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        0 AS t_seq,
        vorschriften_dokument.t_id AS vorschriften_dokument_textimweb
    FROM
        arp_npl_oereb.vorschriften_dokument AS vorschriften_dokument,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
        ) AS basket_dataset
    WHERE
        vorschriften_dokument.t_datasetname = 'ch.so.arp.nutzungsplanung'
    RETURNING *
)
,
localiseduri AS 
(
    SELECT 
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass) AS t_id,
        basket_dataset.basket_t_id,
        basket_dataset.datasetname,
        0 AS t_seq,
        'de' AS alanguage,
        CAST('https://geo.so.ch/docs/ch.so.arp.zonenplaene/Zonenplaene_pdf/' || COALESCE(rechtsvorschrften_dokument.textimweb, '404.pdf') AS TEXT) AS atext,
        multilingualuri.t_id AS multilingualuri_localisedtext
    FROM
        arp_npl.rechtsvorschrften_dokument AS rechtsvorschrften_dokument
        RIGHT JOIN multilingualuri 
        ON multilingualuri.vorschriften_dokument_textimweb = rechtsvorschrften_dokument.t_id,
        (
            SELECT
                basket.t_id AS basket_t_id,
                dataset.datasetname AS datasetname               
            FROM
                arp_npl_oereb.t_ili2db_dataset AS dataset
                LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
                ON basket.dataset = dataset.t_id
            WHERE
                dataset.datasetname = 'ch.so.arp.nutzungsplanung'                 
        ) AS basket_dataset
)
INSERT INTO
    arp_npl_oereb.localiseduri
    (
        t_id,
        t_basket,
        t_datasetname,
        t_seq,
        alanguage,
        atext,
        multilingualuri_localisedtext
    )
    SELECT 
        t_id,
        basket_t_id,
        datasetname,
        t_seq,
        alanguage,
        atext,
        multilingualuri_localisedtext
    FROM 
        localiseduri
;

/*
 * Umbau der Geometrien, die Inhalt des ÖREB-Katasters sind.
 * 
 * (1) Es werden nicht alle Geometrien der jeweiligen
 * Nutzungsebene kopiert, sondern nur diejenigen, die Inhalt 
 * des ÖREB-Katasters sind. Dieser Filter wird bei Umbau 
 * des NPL-Typs gesetzt.
 * 
 * (2) Die zuständige Stelle ist identisch mit der zuständigen
 * Stelle der Eigentumsbeschränkung.
 * 
 * (3) Es werden teilweise nicht sehr transparente und robuste Geometriebereinigungen
 * durchgeführt. Dies muss solange gemacht werden, bis die Inputgeometrie 100% 
 * sauber sind.
 */

INSERT INTO
    arp_npl_oereb.transferstruktur_geometrie
    (
        t_id,
        t_basket,
        t_datasetname,
        flaeche_lv95,
        rechtsstatus,
        publiziertab,
        eigentumsbeschraenkung,
        zustaendigestelle
    )
    SELECT 
        nutzung.t_id,
        basket_dataset.basket_t_id AS t_basket,
        basket_dataset.datasetname AS t_datasetname,
        --ST_MakeValid(ST_RemoveRepeatedPoints(ST_SnapToGrid(nutzung.geometrie, 0.001))) AS flaeche_lv95,
        ST_GeometryN(ST_CollectionExtract(ST_MakeValid(ST_RemoveRepeatedPoints(ST_SnapToGrid(nutzung.geometrie, 0.001))), 3), 1) AS flaeche_lv95,
        nutzung.rechtsstatus AS rechtsstatus,
        nutzung.publiziertab AS publiziertab,
        eigentumsbeschraenkung.t_id AS eigentumsbeschraenkung,
        eigentumsbeschraenkung.zustaendigestelle AS zustaendigestelle
    FROM
    (
        -- Grundnutzung
        SELECT
            t_id,    
            geometrie AS geometrie,
            rechtsstatus AS rechtsstatus,
            publiziertab AS publiziertab,
            typ_grundnutzung AS typ_nutzung
        FROM
            arp_npl.nutzungsplanung_grundnutzung
        WHERE
            rechtsstatus = 'inKraft'

        UNION ALL
        
        -- Überlagernd (Fläche) + Sondernutzungspläne + Lärmempfindlichkeitsstufen
        SELECT
            t_id,    
            ST_Buffer(geometrie, 0) AS geometrie, -- TODO: fixme
            rechtsstatus AS rechtsstatus,
            publiziertab AS publiziertab,
            typ_ueberlagernd_flaeche AS typ_nutzung            
        FROM
            arp_npl.nutzungsplanung_ueberlagernd_flaeche
        WHERE
            rechtsstatus = 'inKraft'
        AND
            ST_IsEmpty(ST_Buffer(geometrie, 0)) IS FALSE
        AND
            ST_Area(geometrie) > 0.0001

    ) AS nutzung
    INNER JOIN arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
    ON nutzung.typ_nutzung = eigentumsbeschraenkung.t_id,
    (
        SELECT
            basket.t_id AS basket_t_id,
            dataset.datasetname AS datasetname               
        FROM
            arp_npl_oereb.t_ili2db_dataset AS dataset
            LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
            ON basket.dataset = dataset.t_id
        WHERE
            dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
    ) AS basket_dataset
;

INSERT INTO
    arp_npl_oereb.transferstruktur_geometrie
    (
        t_id,
        t_basket,
        t_datasetname,
        linie_lv95,
        rechtsstatus,
        publiziertab,
        eigentumsbeschraenkung,
        zustaendigestelle
    )
    SELECT 
        nutzung.t_id,
        basket_dataset.basket_t_id AS t_basket,
        basket_dataset.datasetname AS t_datasetname,
        ST_MakeValid(ST_RemoveRepeatedPoints(ST_SnapToGrid(nutzung.geometrie, 0.001))) AS linie_lv95,
        nutzung.rechtsstatus AS rechtsstatus,
        nutzung.publiziertab AS publiziertab,
        eigentumsbeschraenkung.t_id AS eigentumsbeschraenkung,
        eigentumsbeschraenkung.zustaendigestelle AS zustaendigestelle
    FROM
    (
        -- Überlagernd (Linie)
        SELECT
            t_id,    
            geometrie AS geometrie,
            rechtsstatus AS rechtsstatus,
            publiziertab AS publiziertab,
            typ_ueberlagernd_linie AS typ_nutzung            
        FROM
            arp_npl.nutzungsplanung_ueberlagernd_linie
        WHERE
            rechtsstatus = 'inKraft'

        UNION ALL

        -- Baulinien
        SELECT
            t_id,    
            geometrie AS geometrie,
            rechtsstatus AS rechtsstatus,
            publiziertab AS publiziertab,
            typ_erschliessung_linienobjekt AS typ_nutzung            
        FROM
            arp_npl.erschlssngsplnung_erschliessung_linienobjekt
        WHERE
            rechtsstatus = 'inKraft'
        AND 
            ST_IsValid(geometrie)
    ) AS nutzung
    INNER JOIN arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
    ON nutzung.typ_nutzung = eigentumsbeschraenkung.t_id,
    (
        SELECT
            basket.t_id AS basket_t_id,
            dataset.datasetname AS datasetname               
        FROM
            arp_npl_oereb.t_ili2db_dataset AS dataset
            LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
            ON basket.dataset = dataset.t_id
        WHERE
            dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
    ) AS basket_dataset
;

INSERT INTO
    arp_npl_oereb.transferstruktur_geometrie
    (
        t_id,
        t_basket,
        t_datasetname,
        punkt_lv95,
        rechtsstatus,
        publiziertab,
        eigentumsbeschraenkung,
        zustaendigestelle
    )
    SELECT 
        nutzung.t_id,
        basket_dataset.basket_t_id AS t_basket,
        basket_dataset.datasetname AS t_datasetname,
        ST_MakeValid(ST_RemoveRepeatedPoints(ST_SnapToGrid(nutzung.geometrie, 0.001))) AS punkt_lv95,
        nutzung.rechtsstatus AS rechtsstatus,
        nutzung.publiziertab AS publiziertab,
        eigentumsbeschraenkung.t_id AS eigentumsbeschraenkung,
        eigentumsbeschraenkung.zustaendigestelle AS zustaendigestelle
    FROM
    (
        -- Überlagernd (Punkt)
        SELECT
            t_id,    
            geometrie AS geometrie,
            rechtsstatus AS rechtsstatus,
            publiziertab AS publiziertab,
            typ_ueberlagernd_punkt AS typ_nutzung            
        FROM
            arp_npl.nutzungsplanung_ueberlagernd_punkt
        WHERE
            rechtsstatus = 'inKraft'
    ) AS nutzung
    INNER JOIN arp_npl_oereb.transferstruktur_eigentumsbeschraenkung AS eigentumsbeschraenkung
    ON nutzung.typ_nutzung = eigentumsbeschraenkung.t_id,
    (
        SELECT
            basket.t_id AS basket_t_id,
            dataset.datasetname AS datasetname               
        FROM
            arp_npl_oereb.t_ili2db_dataset AS dataset
            LEFT JOIN arp_npl_oereb.t_ili2db_basket AS basket
            ON basket.dataset = dataset.t_id
        WHERE
            dataset.datasetname = 'ch.so.arp.nutzungsplanung' 
    ) AS basket_dataset
;

-- Falls Darstellungsdienste wieder gelöscht werden müssen.
-- Das ist aber nur die halbe Miete. Das eigentliche Löschen
-- fehlt noch.
-- SELECT 
--     *
-- FROM 
--     arp_npl_oereb.localiseduri AS localiseduri
--     LEFT JOIN 
--     (
--         SELECT 
--             DISTINCT ON (subthema) 
--             subthema AS wmslayer
--         FROM 
--             arp_npl_oereb.transferstruktur_eigentumsbeschraenkung 
--         WHERE
--             subthema IS NOT NULL
            
--         UNION ALL
            
--         SELECT 
--             DISTINCT ON (thema)
--             thema AS wmslayer
--         FROM 
--             arp_npl_oereb.transferstruktur_eigentumsbeschraenkung 
--         WHERE
--             subthema IS NULL
--     ) AS existingwmslayername 
--     ON localiseduri.atext LIKE '%'||existingwmslayername.wmslayer||'%'
-- WHERE
--     t_datasetname = 'ch.so.arp.nutzungsplanung'
-- AND 
--     atext LIKE '%/wms/oereb%'

