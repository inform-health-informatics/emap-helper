


DROP VIEW IF EXISTS flow.icu_now;
CREATE OR REPLACE VIEW flow.icu_now AS
SELECT 
      v.mrn_id,
      vd.hospital_visit_id,
      m.mrn,
      v.encounter AS csn,
      p.sex,
      floor(abs(v.admission_time::date - p.date_of_birth)::numeric / 365.25) AS age,
      p.lastname,
      p.firstname,
      vd.admission_time,
      vd.discharge_time,
      l.location_string,
      split_part(l.location_string::text, '^'::text, 1) AS ward,
      split_part(l.location_string::text, '^'::text, 3) AS bed
     FROM star_test.location_visit vd
       JOIN star_test.hospital_visit v ON vd.hospital_visit_id = v.hospital_visit_id
       JOIN star_test.location l ON vd.location_id = l.location_id
       JOIN star_test.core_demographic p ON v.mrn_id = p.mrn_id
       JOIN star_test.mrn m ON v.mrn_id = m.mrn_id
    WHERE v.patient_class::text = 'INPATIENT'::text AND (l.location_string::text ~~ 'T03%'::text OR l.location_string::text ~~ 'WSCC%'::text OR l.location_string::text ~~ 'null%'::text) AND vd.admission_time IS NOT NULL AND vd.
    discharge_time IS NULL AND v.admission_time IS NOT NULL AND v.discharge_time IS NULL AND p.date_of_death IS NULL
    ORDER BY l.location_string;
