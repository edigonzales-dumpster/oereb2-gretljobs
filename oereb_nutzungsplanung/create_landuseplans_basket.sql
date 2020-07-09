WITH dataset AS 
(
    INSERT INTO 
        arp_npl_oereb.t_ili2db_dataset
        (
            t_id,
            datasetname 
        )
    SELECT
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass),
        CAST('ch.so.arp.nutzungsplanung' AS TEXT)
    RETURNING *
)
INSERT INTO
    arp_npl_oereb.t_ili2db_basket 
    (
        t_id,
        dataset,
        topic,
        attachmentkey
    )
    SELECT 
        nextval('arp_npl_oereb.t_ili2db_seq'::regclass),
        dataset.t_id,
        CAST('OeREBKRMtrsfr_V2_0.Transferstruktur' AS TEXT),
        CAST('ch.so.arp.nutzungsplanung.oereb.xtf' AS TEXT)
    FROM 
        dataset 
;