SELECT
       programEnrollmentsTable.incident_date,
       programEnrollmentsTable.date_created       AS enrollment_date_created,
       programEnrollmentsTable.program_unique_id  AS program_unique_id,
       eventsTable.*,
       orgTracker.id                              AS orgunit_id,
       insTracker.instance_id
FROM (SELECT prog.*
        FROM %s prog
        INNER JOIN marker enrollment_marker ON prog.date_created::TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')
                AND category = 'enrollment' AND program_name =  '%s') AS  programEnrollmentsTable
FULL OUTER JOIN (SELECT event.*
                    FROM %s event
                    INNER JOIN marker event_marker ON event.date_created::TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')
                    AND category = 'event' AND program_name =  '%s') AS  eventsTable
ON  programEnrollmentsTable."Patient_Identifier" =  eventsTable."Patient_Identifier"
    AND eventsTable.enrollment_date = programEnrollmentsTable.enrollment_date
INNER JOIN orgunit_tracker orgTracker ON eventsTable."OrgUnit" = orgTracker.orgunit
INNER JOIN instance_tracker insTracker ON eventsTable."Patient_Identifier" = insTracker.patient_id
LEFT JOIN enrollment_tracker enrolTracker ON enrolTracker.instance_id = insTracker.instance_id
WHERE programEnrollmentsTable.status = 'COMPLETED'
  AND enrolTracker.instance_id IS NULL;
