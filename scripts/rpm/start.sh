#!/bin/sh
nohup $JAVA_HOME/bin/java  -jar $SERVER_OPTS /opt/mart-dhis2-sync/lib/mart-dhis2-sync.jar --spring.config.location="/opt/mart-dhis2-sync/properties/" >> /var/log/mart-dhis2-sync/mart-dhis2-sync.log 2>&1 &
echo $! > /var/run/mart-dhis2-sync/mart-dhis2-sync.pid