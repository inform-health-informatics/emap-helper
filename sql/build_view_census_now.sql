-- 21 Nov
-- Query for inspecting ward census 'now'

DROP VIEW IF EXISTS flow.census_now;
CREATE OR REPLACE VIEW flow.census_now AS
WITH
loc AS (
SELECT
     location_id
    ,location_string
    ,SPLIT_PART(location_string,'^',1) ward
    ,SPLIT_PART(location_string,'^',3) bed
FROM star_test.location
WHERE SPLIT_PART(location_string,'^',3) NOT IN ('null', 'WAIT')
),
wide AS (
SELECT 
     vd.hospital_visit_id
    ,vd.location_visit_id
    ,m.mrn
    ,p.lastname
    ,p.firstname
    ,v.encounter
    ,vd.admission_time bed_in
    ,vd.discharge_time bed_dc
    ,v.admission_time hosp_in
    ,v.discharge_time hosp_dc
    ,loc.location_string
    ,loc.ward
    ,loc.bed
FROM star_test.location_visit vd
JOIN loc ON vd.location_id = loc.location_id
JOIN star_test.hospital_visit v ON vd.hospital_visit_id = v.hospital_visit_id
JOIN star_test.core_demographic p ON v.mrn_id = p.mrn_id
JOIN star_test.mrn m ON p.mrn_id = m.mrn_id
)
SELECT 
*
FROM wide
WHERE 
    wide.bed_in IS NOT NULL AND wide.bed_dc IS NULL
AND wide.hosp_in IS NOT NULL AND wide.hosp_dc IS NULL
--AND wide.ward = 'T12N'
;
