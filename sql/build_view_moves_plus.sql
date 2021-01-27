-- 2020-11-23 THIS DOES NOT WORK!
-- dropping a whole load of recent updates
-- 2020-11-23
-- fire up psql in a tmux pane below using ```psql uds``` given that all secrets are in .env
SET search_path to star_test, public;
-- now try and create a performant view of bed moves with previous and next
-- use this view to perform analysis
-- create indicator variables for things like ward move, internal move, etc.

-- include patient details

DROP VIEW IF EXISTS flow.moves_plus;
CREATE VIEW flow.moves_plus AS
-- prep location
WITH
loc AS (
SELECT
     location_id
    ,SPLIT_PART(location_string,'^',1) ward
    ,SPLIT_PART(location_string,'^',2) room
    ,SPLIT_PART(location_string,'^',3) bed
    ,CASE WHEN SPLIT_PART(location_string,'^',1) = 'ED' THEN true END in_ED
    ,CASE WHEN SPLIT_PART(location_string,'^',3) = 'THR' THEN true END in_theatre
    ,CASE WHEN SPLIT_PART(location_string,'^',1) IN ('T03', 'P03CV') THEN true END in_ICU   -- just ICU in the tower
    ,CASE WHEN SUBSTR(location_string,1,2) IN ('T0', 'T1', 'THP3') THEN true END in_tower   -- THP3 includes podium theatres
FROM location
--WHERE SPLIT_PART(location_string,'^',3) != 'null'
),
-- prep visit detail
vd AS (
SELECT 
     vd.hospital_visit_id
    ,vd.location_visit_id
    ,m.mrn
    ,p.lastname
    ,p.firstname
    ,v.encounter
    ,v.patient_class
    ,vd.admission_time bed_admit
    ,vd.discharge_time bed_dc
    ,loc.*
    /* ,loc.ward */
    /* ,loc.room */
    /* ,loc.bed */
    ,LAG(loc.ward,1) OVER (
        PARTITION BY vd.hospital_visit_id
        ORDER BY vd.admission_time ASC
        ) ward_lag1
    ,LEAD(loc.ward,1) OVER (
        PARTITION BY vd.hospital_visit_id
        ORDER BY vd.admission_time ASC
        ) ward_lead1
FROM star_test.location_visit as vd 
JOIN loc ON vd.location_id = loc.location_id
JOIN star_test.hospital_visit v ON vd.hospital_visit_id = v.hospital_visit_id
JOIN star_test.core_demographic p ON v.mrn_id = p.mrn_id
JOIN star_test.mrn m ON p.mrn_id = m.mrn_id
)
SELECT 
     vd.* 
    ,CASE WHEN ward_lag1 != ward THEN true END ward_admit
    ,CASE WHEN ward_lead1 != ward THEN true END ward_discharge
FROM vd
--LIMIT 3
;
