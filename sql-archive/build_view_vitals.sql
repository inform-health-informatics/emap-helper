-- Steve Harris
-- Created 2021-01-27
-- produce a realtime view of vitals across the tower
-- returns a week's worth of data (much more and slows down too much)

-- work with
-- spo2 2273987897
-- pulse 2273987904
-- resp rate 2273987924
-- bp 2273987901
-- temp 2273987915
-- room air or oxygen 2273987935

-- LOG
-- 2021-01-27 created by duplicating from the o2 demand version



-- TODO
-- todo: finesse the calculation to include respiratory rate and machine type

SET search_path to star_a, public;
-- this moves all operations into memory
SET work_mem='256MB';

create or replace function flow.locf_s(a float, b float)
returns float
language sql
as '
  SELECT COALESCE(b, a)
';

--drop aggregate if exists flow.locf(float);
--create aggregate flow.locf(float) (
--  sfunc = flow.locf_s,
--  stype = float
--);

DROP VIEW IF EXISTS flow.vitals;
CREATE VIEW flow.vitals AS
WITH wide AS (SELECT 
* 
FROM 
	crosstab(
	$$SELECT 
		  hospital_visit_id
		, observation_datetime
		, CASE 
			WHEN visit_observation_type_id = 2273987897 THEN 'spo2'
			WHEN visit_observation_type_id = 2273987935 THEN 'ra_o2'
		 	WHEN visit_observation_type_id = 2273987904 THEN 'hr'
		 	WHEN visit_observation_type_id = 2273987924 THEN 'rr'
		 	WHEN visit_observation_type_id = 2273987901 THEN 'bp'
		 	WHEN visit_observation_type_id = 2273987915 THEN 'tempf'
			END AS vital_obs
		, CASE 
			WHEN visit_observation_type_id = 2273987897 THEN value_as_real::TEXT
			WHEN visit_observation_type_id = 2273987935 THEN value_as_text
		 	WHEN visit_observation_type_id = 2273987904 THEN value_as_real::TEXT
		 	WHEN visit_observation_type_id = 2273987924 THEN value_as_real::TEXT
		 	WHEN visit_observation_type_id = 2273987901 THEN value_as_text
		 	WHEN visit_observation_type_id = 2273987915 THEN value_as_real::TEXT
			END AS vital_val
		FROM star_a.visit_observation
		WHERE visit_observation_type_id IN (
			 2273987897 
			,2273987935 
		 	,2273987904
		 	,2273987924
		 	,2273987901
		 	,2273987915
			) 
			AND
			observation_datetime > NOW() - '2 DAYS'::INTERVAL			
		ORDER BY 2,1
	$$
	, $$VALUES 
          ('spo2')
		, ('ra_o2')
		, ('hr')
		, ('rr')
		, ('bp')
		, ('tempf')
		$$
	)
	AS ct(
		  "hospital_visit_id" BIGINT 
		, "observation_datetime" TIMESTAMP
		, "spo2" int
		, "ra_o2" text
		, "hr" int
		, "rr" int
		, "bp" TEXT
		, "tempf" REAL
	)
ORDER BY hospital_visit_id, observation_datetime DESC
),
vitalsi AS (
	SELECT 
	 	  wide.hospital_visit_id
	 	, DATE_TRUNC('HOUR', wide.observation_datetime) observation_datetime
	 	, wide.spo2
	 	, CASE WHEN wide.ra_o2 = 'Supplemental Oxygen' THEN 1 ELSE 0 END AS o2supp
	 	, wide.hr
	 	, wide.rr
	 	, SPLIT_PART(wide.bp, '/', 1)::int AS sbp
	 	, (wide.tempf - 32) * 5/9 AS tempc
		, vd.location_string 
	FROM wide
	INNER JOIN LATERAL (
			SELECT 
				  vd.location_id
				, vd.discharge_time
				, loc.location_string
			FROM
				star_a.location_visit vd
			INNER JOIN 
			 	(SELECT 
			 		  location_id
			 		, location_string
				 FROM star_a.location) loc			  
				 ON vd.location_id = loc.location_id
			WHERE 
			 	wide.hospital_visit_id = vd.hospital_visit_id
			 	AND wide.observation_datetime >= vd.admission_time
			 	ORDER BY vd.admission_time DESC
			 	LIMIT 1
			)  vd ON TRUE
	-- for debugging; work with a single patient
	--WHERE hospital_visit_id = 2332134153
	WHERE SUBSTRING(SPLIT_PART(location_string, '^', 1), '^(\D+)\d*') IN ('T', 'P', 'HS')
), 
vitals AS (
    SELECT
          hospital_visit_id
        , observation_datetime
        , location_string
        , MAX(o2supp) o2supp
        , AVG(hr) hr
        , AVG(rr) rr
        , AVG(sbp) sbp
        , AVG(tempc) tempc
    FROM vitalsi
    GROUP BY hospital_visit_id, location_string, observation_datetime
),
grid AS (
	SELECT 
 		DATE_TRUNC('hour', obs_dt) obs_dt
	FROM GENERATE_SERIES(
		  NOW() - '2 DAYS'::INTERVAL
		, NOW()
		, '1 HOUR'::INTERVAL) obs_dt
),
loc AS (
	SELECT DISTINCT location_string FROM vitals
),
xgrid AS (
	SELECT 
	 location_string
	, obs_dt 
	FROM grid CROSS JOIN loc
),
sparse AS (
SELECT 
	  xgrid.location_string
	, xgrid.obs_dt
	, vitals.hospital_visit_id
	, vitals.o2supp
	, vitals.hr
	, vitals.rr
	, vitals.sbp
	, vitals.tempc
FROM xgrid
LEFT JOIN vitals ON 
	xgrid.location_string = vitals.location_string
	AND
	xgrid.obs_dt = vitals.observation_datetime
),
locf AS (
SELECT
	  os.location_string
	, os.obs_dt

	, o2supp
	, hr
	, rr
	, sbp
	, tempc

	, flow.locf(o2supp) over (PARTITION BY location_string ORDER BY obs_dt) AS o2supp_locf
	, flow.locf(hr) over (PARTITION BY location_string ORDER BY obs_dt) AS hr_locf
	, flow.locf(rr) over (PARTITION BY location_string ORDER BY obs_dt) AS rr_locf
	, flow.locf(sbp) over (PARTITION BY location_string ORDER BY obs_dt) AS sbp_locf
	, flow.locf(tempc) over (PARTITION BY location_string ORDER BY obs_dt) AS tempc_locf
	
	, SPLIT_PART(location_string, '^', 1) ward
	, SUBSTRING(SPLIT_PART(location_string, '^', 1), '^\D+(\d+)\D*')::INTEGER floor_index
	, SUBSTRING(location_string, '.*?(\d+)\D?$')::integer bed_index
FROM sparse os
)
SELECT
*
FROM locf
;
RESET work_mem;
;
SELECT * FROM flow.vitals LIMIT 3;
