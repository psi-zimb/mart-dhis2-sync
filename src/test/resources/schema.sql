DROP TABLE IF EXISTS mapping CASCADE;
CREATE TABLE "public"."mapping"(
  mapping_name text,
  lookup_table json,
  mapping_json json,
  created_by text,
  created_date date,
  modifed_by text,
  modifed_date date
);

DROP TABLE IF EXISTS hts_instance CASCADE;
CREATE TABLE "public"."hts_instance"(
  first_name text,
  last_name text
);

DROP TABLE IF EXISTS instance_tracker CASCADE;
CREATE TABLE "public"."instance_tracker"(
  patient_id text,
  instance_id text PRIMARY KEY,
  created_by text,
  date_created TIMESTAMP
);

DROP TABLE IF EXISTS orgunit_tracker CASCADE;
CREATE TABLE "public"."orgunit_tracker"(
  orgunit text  PRIMARY KEY,
  id text,
  date_created TIMESTAMP
);
