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

/bin/sh /mosquitto/config/dynsec-bootstrap.sh

wait "$broker_pid"
