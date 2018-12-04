SELECT
  enrollmentsTable.incident_date,
  enrollmentsTable.date_created         AS enrollment_date_created,
  enrollmentsTable.program_unique_id    AS program_unique_id,
  enrollmentsTable.status               AS enrollment_status,
  enrollmentsTable.program              AS enrolled_program,
  enrollmentsTable.enrollment_date      AS enr_date,
  enrollmentsTable."Patient_Identifier" AS enrolled_patient_identifier,
  eventsTable.*,
  orgTracker.id                         AS orgunit_id,
  insTracker.instance_id,
  enrolTracker.enrollment_id,
  evntTracker.event_id
FROM (SELECT enrTable.*
      FROM %s enrTable
        INNER JOIN marker enrollment_marker
          ON enrTable.date_created :: TIMESTAMP > COALESCE(enrollment_marker.last_synced_date, '-infinity')
             AND category = 'enrollment' AND program_name = '%s') AS enrollmentsTable
  FULL OUTER JOIN (SELECT evnTable.*
                   FROM %s evnTable
                     INNER JOIN marker event_marker
                       ON evnTable.date_created :: TIMESTAMP > COALESCE(event_marker.last_synced_date, '-infinity')
                          AND category = 'event' AND program_name = '%s') AS eventsTable
    ON enrollmentsTable."Patient_Identifier" = eventsTable."Patient_Identifier"
       AND eventsTable.enrollment_date = enrollmentsTable.enrollment_date
  INNER JOIN orgunit_tracker orgTracker ON eventsTable."OrgUnit" = orgTracker.orgunit
  INNER JOIN instance_tracker insTracker ON eventsTable."Patient_Identifier" = insTracker.patient_id
  LEFT JOIN enrollment_tracker enrolTracker ON enrollmentsTable.program = enrolTracker.program
                                               AND enrolTracker.instance_id = insTracker.instance_id
                                               AND enrolTracker.program_unique_id = enrollmentsTable.program_unique_id :: TEXT
  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id
                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id
                                         AND eventsTable.program = evntTracker.program
                                         AND eventsTable.program_stage = evntTracker.program_stage
WHERE enrollmentsTable.status = 'COMPLETED' OR enrollmentsTable.status = 'CANCELLED'
                                            AND enrolTracker.instance_id IS NOT NULL %s;
