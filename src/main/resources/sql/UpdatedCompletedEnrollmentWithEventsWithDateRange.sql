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
  evntTracker.event_id,
  'updated_completed_enrollment' AS enrollment_type,
  insTable."UIC" AS UIC,
  insTable."Gender",
  insTable."District_of_Birth",
  insTable."Date_of_Birth"
FROM (SELECT enrTable.*
      FROM %s enrTable
          WHERE enrTable.enrollment_date :: DATE <= '%s'
          ) AS enrollmentsTable
  INNER JOIN (SELECT evnTable.*,
                   enrollments.program_unique_id AS event_program_unique_id,
                   enrollments.status            AS event_program_status,
                   enrollments.incident_date     AS event_program_incident_date
                   FROM %s evnTable
                     INNER JOIN %s enrollments ON evnTable."Patient_Identifier" = enrollments."Patient_Identifier"
                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)
                              AND evnTable.patient_program_id = enrollments.program_unique_id
                              AND evnTable.event_date :: DATE BETWEEN '%s' AND '%s'
                         ) AS eventsTable
    ON enrollmentsTable."Patient_Identifier" = eventsTable."Patient_Identifier"
       AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)
  INNER JOIN %s insTable ON enrollmentsTable."Patient_Identifier" = insTable."Patient_Identifier"
    INNER JOIN orgunit_tracker orgTracker ON COALESCE(eventsTable."OrgUnit", enrollmentsTable."OrgUnit") = orgTracker.orgunit
  INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable."Patient_Identifier", enrollmentsTable."Patient_Identifier") = insTracker.patient_id
  LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program
                                               AND enrolTracker.instance_id = insTracker.instance_id
  LEFT JOIN event_tracker evntTracker ON insTracker.instance_id = evntTracker.instance_id
                                         AND eventsTable.event_unique_id :: TEXT = evntTracker.event_unique_id
                                         AND eventsTable.program = evntTracker.program
                                         AND eventsTable.program_stage = evntTracker.program_stage
WHERE (enrollmentsTable.status = 'COMPLETED' OR eventsTable.event_program_status = 'COMPLETED')
                                             %s
                                            order by enrollmentsTable.date_created;
