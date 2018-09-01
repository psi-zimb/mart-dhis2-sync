#!/bin/bash

if [ -f /etc/bahmni-installer/bahmni.conf ]; then
. /etc/bahmni-installer/bahmni.conf
fi

if [ -f /etc/mart-dhis2-sync/mart-dhis.conf ]; then
. /etc/mart-dhis2-sync/mart-dhis.conf
fi

USERID=bahmni
GROUPID=bahmni
/bin/id -g $GROUPID 2>/dev/null
[ $? -eq 1 ]
groupadd bahmni

/bin/id $USERID 2>/dev/null
[ $? -eq 1 ]
useradd -g bahmni bahmni

create_mart_dhis_directories() {
    if [ ! -d /opt/mart-dhis2-sync/log/ ]; then
        mkdir -p /opt/mart-dhis2-sync/log/
    fi

    if [ ! -d /opt/mart-dhis2-sync/properties/ ]; then
        mkdir -p /opt/mart-dhis2-sync/properties/
    fi

    if [ ! -d /var/run/mart-dhis2-sync/ ]; then
        mkdir -p /var/run/mart-dhis2-sync
    fi
}

getpostgresjar() {
    if [ ! -f /opt/bahmni-mart/lib/postgresql-42.2.2.jar ]; then
        wget https://jdbc.postgresql.org/download/postgresql-42.2.2.jar -O /opt/bahmni-mart/lib/postgresql-42.2.2.jar
    fi
}

link_directories() {
    #create links
    ln -s /opt/mart-dhis2-sync/log /var/log/mart-dhis2-sync
    ln -s /opt/mart-dhis2-sync/etc /etc/mart-dhis2-sync
    ln -s /opt/mart-dhis2-sync/bin/mart-dhis2-sync /etc/init.d/mart-dhis2-sync
}

manage_permissions() {
    # permissions
    chown -R bahmni:bahmni /opt/mart-dhis2-sync
    chown -R bahmni:bahmni /var/log/mart-dhis2-sync
    chmod -R 777 /var/run/mart-dhis2-sync
}

updating_firewall_rules_to_allow_mart_dhis_service_port() {
    echo "allowing postgres port in firewall rules"
    sudo iptables -A INPUT -p tcp --dport 8061 -j ACCEPT -m comment --comment "MARTDHIS"
    sudo service iptables save
}

run_migrations(){
    echo "Running openmrs liquibase-core-data.xml and liquibase-update-to-latest.xml"
    /opt/mart-dhis2-sync/etc/run-liquibase.sh
}

setupConfFiles() {
    	rm -f /etc/httpd/conf.d/mart-dhis2-sync-ssl.conf
    	cp -f /opt/mart-dhis2-sync/etc/mart-dhis2-sync-ssl.conf /etc/httpd/conf.d/mart-dhis2-sync-ssl.conf
}

link_properties_file() {
    echo "Linking properties file"
    ln -s /opt/mart-dhis2-sync/etc/mart-dhis.properties /opt/mart-dhis2-sync/properties/application.properties
}

chkconfig --add mart-dhis2-sync

setupConfFiles
create_mart_dhis_directories
link_directories
manage_permissions
getpostgresjar
run_migrations
setupConfFiles
link_properties_file
updating_firewall_rules_to_allow_mart_dhis_service_port