#!/bin/sh
set -eu

data_dir=/mosquitto/data
config_file=$data_dir/dynamic-security.json

mkdir -p "$data_dir"
chown mosquitto:mosquitto "$data_dir"
mosquitto_ctrl dynsec init "$config_file" admin admin
chown mosquitto:mosquitto "$config_file"
chmod 640 "$config_file"

mosquitto -c /mosquitto/config/mosquitto-dynsec.conf &
broker_pid=$!

attempt=0
until mosquitto_ctrl -h 127.0.0.1 -p 1883 -u admin -P admin dynsec getClient admin >/dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        echo "Mosquitto Dynamic Security did not become ready" >&2
        exit 1
    fi
    sleep 1
done

dynsec() {
    mosquitto_ctrl -h 127.0.0.1 -p 1883 -u admin -P admin dynsec "$@"
}

dynsec setDefaultACLAccess publishClientSend allow
dynsec setDefaultACLAccess subscribe allow
dynsec createClient zmqtt-mosquitto -p zmqtt-mosquitto
dynsec createRole zmqtt-tests
dynsec addClientRole zmqtt-mosquitto zmqtt-tests 10
dynsec addRoleACL zmqtt-tests subscribePattern '#' allow 10
dynsec addRoleACL zmqtt-tests publishClientSend '#' allow 10
dynsec addRoleACL zmqtt-tests publishClientSend 'zmqtt/e2e/denied' deny 20
dynsec addRoleACL zmqtt-tests unsubscribePattern '#' allow 10
dynsec addRoleACL zmqtt-tests unsubscribePattern 'zmqtt/unsuback/denied/#' deny 20

wait "$broker_pid"
