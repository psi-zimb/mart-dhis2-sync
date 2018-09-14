SELECT lt.*,
  CASE WHEN i.instance_id is NULL THEN '' ELSE i.instance_id END AS instance_id,
  CASE WHEN o.id is NULL THEN '' ELSE o.id END AS orgunit_id
FROM %s lt
  LEFT JOIN instance_tracker i ON  lt."Patient_Identifier" = i.patient_id
  LEFT JOIN orgunit_tracker o ON  lt."OrgUnit" = o.orgUnit

WHERE i.instance_id IS NOT NULL;

