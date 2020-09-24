SELECT
  COALESCE(enrollmentsTable.program, eventsTable.program)   AS program,
  insTracker.instance_id
FROM (SELECT enrTable.*
      FROM %s enrTable
          WHERE enrTable.enrollment_date :: DATE <= '%s'
          ) AS enrollmentsTable
INNER JOIN (SELECT evnTable.*,
                   enrollments.program_unique_id AS event_program_unique_id
                   FROM %s evnTable
                   INNER JOIN %s enrollments ON evnTable."Patient_Identifier" = enrollments."Patient_Identifier"
                              AND evnTable.enrollment_date = COALESCE(enrollments.enrollment_date, evnTable.enrollment_date)
                   AND  evnTable.event_date :: DATE BETWEEN '%s' AND '%s'
                ) AS eventsTable
    ON enrollmentsTable."Patient_Identifier" = eventsTable."Patient_Identifier"
    AND eventsTable.enrollment_date = COALESCE(enrollmentsTable.enrollment_date, eventsTable.enrollment_date)
INNER JOIN instance_tracker insTracker ON COALESCE(eventsTable."Patient_Identifier", enrollmentsTable."Patient_Identifier") = insTracker.patient_id
LEFT JOIN enrollment_tracker enrolTracker ON COALESCE(enrollmentsTable.program, eventsTable.program) = enrolTracker.program
                                          AND enrolTracker.instance_id = insTracker.instance_id
                                          AND enrolTracker.program_unique_id = COALESCE(enrollmentsTable.program_unique_id, eventsTable.event_program_unique_id) :: TEXT
                                          order by enrollmentsTable.date_created;
