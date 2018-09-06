SELECT lt.*, CASE WHEN i.instance_id is NULL THEN '' ELSE i.instance_id END as instance_id
FROM %s lt LEFT join instance_tracker i ON  lt."Patient_Identifier" = i.patient_id
WHERE date_created > COALESCE((SELECT last_synced_date
                                     FROM marker
                                     WHERE category='%s' AND program_name='%s'), '-infinity');
