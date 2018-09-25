SELECT event.*,
  enrTracker.instance_id,
  enrTracker.enrollment_id,
  orgTracker.id as orgunit_id
FROM hiv_event_view event
INNER JOIN orgunit_tracker orgTracker ON event."OrgUnit" = orgTracker.orgunit
INNER JOIN instance_tracker insTracker ON event."Patient_Identifier" = insTracker.patient_id
INNER JOIN enrollment_tracker enrTracker ON insTracker.instance_id = enrTracker.instance_id
WHERE event.date_created > COALESCE((SELECT last_synced_date
  FROM marker
  WHERE category='event' AND program_name='%s'), '-infinity');