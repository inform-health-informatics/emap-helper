-- 2021-03-06
-- V simple view that finds recent observations for current inpatients
-- NB: visit_observation_type ids vary from star_a to star_test etc.

select
  -- observation details
   ob.visit_observation_id
  ,ob.hospital_visit_id
  ,ob.observation_datetime
  ,ob.unit 
  ,ob.value_as_real
  ,ob.value_as_text
  ,ob.visit_observation_type_id
  ,ot.id_in_application

from
  star.visit_observation ob
-- observation look-up
left join
  star.visit_observation_type ot
  on ob.visit_observation_type_id = ot.visit_observation_type

where
ob.observation_datetime > NOW() - '1 DAYS'::INTERVAL	
and
ot.id_in_application in 

  (
  '10' --'SpO2' -- 602063230
  ,'5' --'BP'   --  602063234
  ,'3040109304' --'Room Air or Oxygen' -- 602063268
  ,'6' --'Temp' -- 62063248
  ,'8' --'Pulse' -- 602063237
  ,'9' --'Resp'   -- 602063257
  ,'28315' -- NEWS2 SpO2 scale 1
  ,'28315' -- NEWS2 SpO2 scale 1
  ,'6466' -- Level of consciousness
)

;

