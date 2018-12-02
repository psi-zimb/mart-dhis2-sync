SELECT enrTable.*, insTracker.instance_id, orgTracker.id as orgunit_id
FROM %s enrTable
       INNER JOIN orgunit_tracker orgTracker ON orgTracker.orgUnit = enrTable."OrgUnit"
       INNER JOIN instance_tracker insTracker ON insTracker.patient_id = enrTable."Patient_Identifier"
       LEFT JOIN enrollment_tracker enrolTracker ON enrolTracker.instance_id = insTracker.instance_id
WHERE enrTable.date_created :: TIMESTAMP > COALESCE((SELECT last_synced_date
                                                     FROM marker
                                                     WHERE category = 'enrollment'
                                                       AND program_name = '%s'), '-infinity')
  AND enrTable.status = 'COMPLETED'
  AND enrolTracker.instance_id IS NULL
  AND enrTable.date_created :: TIMESTAMP <= '%s';
