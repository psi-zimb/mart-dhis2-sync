SELECT event.*,
       et.instance_id,
       et.enrollment_id,
       ot.id as orgunit_id
FROM %s event
INNER JOIN orgunit_tracker ot ON event."OrgUnit" = ot.orgunit
INNER JOIN instance_tracker it ON event."Patient_Identifier" = it.patient_id
INNER JOIN enrollment_tracker et ON it.instance_id = et.instance_id