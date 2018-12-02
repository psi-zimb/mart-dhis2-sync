SELECT enrTable.incident_date,
       enrTable.date_created       AS enrollment_date_created,
       enrTable.program_unique_id  AS program_unique_id,
       evnTable.*,
       orgTracker.id                              AS orgunit_id,
       insTracker.instance_id
FROM %s enrTable
       LEFT JOIN %s evnTable ON evnTable."Patient_Identifier" = enrTable."Patient_Identifier" AND
                                                      evnTable.enrollment_date = enrTable.enrollment_date
       INNER JOIN instance_tracker insTracker ON insTracker.patient_id = enrTable."Patient_Identifier"
       INNER JOIN orgunit_tracker orgTracker ON orgTracker.orgUnit = enrTable."OrgUnit"
       LEFT JOIN enrollment_tracker enrTracker
         ON enrTable.program = enrTracker.program AND enrTracker.instance_id = insTracker.instance_id
              AND enrTracker.program_unique_id = enrTable.program_unique_id :: text
WHERE enrTable.date_created :: TIMESTAMP > COALESCE((SELECT last_synced_date FROM marker WHERE category = 'enrollment'
                                                                                           AND program_name = '%s'),
                                                    '-infinity')
  AND enrTable.status = 'COMPLETED'
  AND enrTracker.instance_id IS NULL;
