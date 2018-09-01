#!/bin/sh
set -e -x

if [ -f /etc/mart-dhis2-sync/mart-dhis.conf ]; then
. /etc/mart-dhis2-sync/mart-dhis.conf
fi


liquibase --driver='org.postgresql.Driver' --classpath=$postgresql_jar_location/$postgresql_jar_name --changeLogFile=$liquibase_changelog_path --defaultSchemaName='public' --url=jdbc:postgresql://$postgres_db_server:$analytics_db_port/$analytics_db_name --username=$analytics_db_user --password=$analytics_db_password update >> /bahmni_temp/logs/bahmni_deploy.log 2>> /bahmni_temp/logs/bahmni_deploy.log