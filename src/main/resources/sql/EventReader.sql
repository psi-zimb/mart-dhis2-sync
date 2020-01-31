SELECT event.*,
  enrTracker.instance_id,
  enrTracker.enrollment_id,
  orgTracker.id as orgunit_id,
  event_tracker.event_id AS event_id
FROM %s event
INNER JOIN %s enrollment ON event."Patient_Identifier" = enrollment."Patient_Identifier" AND event.enrollment_date = enrollment.enrollment_date
INNER JOIN orgunit_tracker orgTracker ON event."OrgUnit" = orgTracker.orgunit
INNER JOIN instance_tracker insTracker ON event."Patient_Identifier" = insTracker.patient_id
INNER JOIN enrollment_tracker enrTracker ON insTracker.instance_id = enrTracker.instance_id
  AND enrollment.program_unique_id::text = enrTracker.program_unique_id AND event.program = enrTracker.program
LEFT JOIN event_tracker ON insTracker.instance_id = event_tracker.instance_id
  AND event.event_unique_id::text = event_tracker.event_unique_id AND event.program = event_tracker.program AND event.program_stage = event_tracker.program_stage
WHERE event.date_created::TIMESTAMP > COALESCE((SELECT last_synced_date
  FROM marker
  WHERE category='event' AND program_name='%s'), '-infinity')
  order by event.date_created;
