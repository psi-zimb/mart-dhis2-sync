SELECT mappedTable.*, insTracker.instance_id, orgTracker.id as orgunit_id,
  CASE WHEN enrTracker.enrollment_id is NULL THEN '' ELSE enrTracker.enrollment_id END AS enrollment_id
FROM %s mappedTable
INNER JOIN instance_tracker insTracker
  ON insTracker.patient_id = mappedTable."Patient_Identifier"
INNER JOIN orgunit_tracker orgTracker
  ON orgTracker.orgUnit = mappedTable."OrgUnit"
LEFT JOIN enrollment_tracker enrTracker
  ON enrTracker.instance_id = insTracker.instance_id
  AND date(enrTracker.program_start_date) = date(mappedTable.enrollment_date)
  WHERE mappedTable.date_created > COALESCE((SELECT last_synced_date
                                    FROM marker
                                    WHERE category='enrollment' AND program_name='%s'), '-infinity');