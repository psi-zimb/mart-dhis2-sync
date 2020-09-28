SELECT enrTable.incident_date,
       enrTable.date_created       AS enrollment_date_created,
       enrTable.program_unique_id  AS program_unique_id,
       enrTable.program            AS enrolled_program,
       enrTable.enrollment_date    AS enr_date,
       enrTable.status             AS enrollment_status,
       enrTable."Patient_Identifier" AS enrolled_patient_identifier,
       evnTable.*,
       orgTracker.id               AS orgunit_id,
       insTracker.instance_id,
       'new_completed_enrollment' AS enrollment_type,
       insTable."UIC" AS UIC,
       insTable."Mothers_First_Name",
       insTable."Are_you_Twin",
       insTable."Last_Name",
       insTable."Gender",
       insTable."District_of_Birth",
       insTable."Date_of_Birth"
FROM %s enrTable
       LEFT JOIN %s evnTable ON evnTable."Patient_Identifier" = enrTable."Patient_Identifier" AND
                                                      evnTable.enrollment_date = enrTable.enrollment_date
                                                      AND evnTable.patient_program_id = enrTable.program_unique_id
       INNER JOIN %s insTable ON enrTable."Patient_Identifier" = insTable."Patient_Identifier"
       INNER JOIN instance_tracker insTracker ON insTracker.patient_id = enrTable."Patient_Identifier"
       INNER JOIN orgunit_tracker orgTracker ON  COALESCE(evnTable."OrgUnit", enrTable."OrgUnit") = orgTracker.orgUnit
       LEFT JOIN enrollment_tracker enrTracker
         ON enrTable.program = enrTracker.program AND enrTracker.instance_id = insTracker.instance_id
              AND enrTracker.program_unique_id = enrTable.program_unique_id :: text
WHERE enrTable.date_created :: DATE BETWEEN '%s' AND '%s'
  AND enrTracker.instance_id IS NULL
  AND (enrTable.status = 'COMPLETED') order by enrTable.date_created;