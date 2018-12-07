SELECT insTracker.instance_id, mappedTable.program
FROM %s mappedTable
INNER JOIN instance_tracker insTracker
  ON insTracker.patient_id = mappedTable."Patient_Identifier"
INNER JOIN orgunit_tracker orgTracker
  ON orgTracker.orgUnit = mappedTable."OrgUnit"
LEFT JOIN enrollment_tracker enrTracker
  ON mappedTable.program = enrTracker.program
    AND enrTracker.instance_id = insTracker.instance_id
    AND enrTracker.program_unique_id = mappedTable.program_unique_id::text
WHERE mappedTable.date_created::TIMESTAMP > COALESCE((SELECT last_synced_date
                                    FROM marker
                                    WHERE category='enrollment' AND program_name='%s'), '-infinity');
