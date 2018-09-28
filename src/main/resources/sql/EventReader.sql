SELECT event.*,
  enrTracker.instance_id,
  enrTracker.enrollment_id,
  orgTracker.id as orgunit_id
FROM %s event
INNER JOIN %s enrollment ON event."Patient_Identifier" = enrollment."Patient_Identifier"
INNER JOIN orgunit_tracker orgTracker ON event."OrgUnit" = orgTracker.orgunit
INNER JOIN instance_tracker insTracker ON event."Patient_Identifier" = insTracker.patient_id
INNER JOIN enrollment_tracker enrTracker ON insTracker.instance_id = enrTracker.instance_id
  AND enrollment.program_unique_id = enrTracker.program_unique_id AND event.program = enrTracker.program
LEFT JOIN event_tracker ON insTracker.instance_id = event_tracker.instance_id
  AND CASE WHEN event.program_unique_id IS NULL THEN date(event.program_start_date)::text ELSE event.program_unique_id END
      = CASE WHEN event_tracker.program_unique_id IS NULL THEN date(event_tracker.program_start_date)::text ELSE event_tracker.program_unique_id END
  AND event.program = event_tracker.program
WHERE event.date_created > COALESCE((SELECT last_synced_date
  FROM marker
  WHERE category='event' AND program_name='%s'), '-infinity');