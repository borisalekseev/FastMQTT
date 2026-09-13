#!/bin/sh
# Grant the Dynamic Security ACLs the broker tests rely on, on every platform
# that runs them: the Mosquitto container, the macOS runner and the Windows one.
#
# MOSQUITTO_CTRL  path to mosquitto_ctrl (default: whatever is on PATH)
# MOSQUITTO_PORT  port the broker listens on (default: 1883)
set -eu

ctrl=${MOSQUITTO_CTRL:-mosquitto_ctrl}
port=${MOSQUITTO_PORT:-1883}

dynsec() {
    "$ctrl" -h 127.0.0.1 -p "$port" -u admin -P admin dynsec "$@" 2>&1
}

grant() {
    dynsec setDefaultACLAccess publishClientSend allow
    dynsec setDefaultACLAccess subscribe allow
    dynsec createGroup zmqtt-anonymous-clients
    dynsec createRole zmqtt-anonymous-role
    dynsec addGroupRole zmqtt-anonymous-clients zmqtt-anonymous-role 10
    dynsec addRoleACL zmqtt-anonymous-role subscribePattern '#' allow 10
    dynsec addRoleACL zmqtt-anonymous-role publishClientSend '#' allow 10
    dynsec setAnonymousGroup zmqtt-anonymous-clients
    dynsec createClient zmqtt-mosquitto -p zmqtt-mosquitto
    dynsec createRole zmqtt-tests
    dynsec addClientRole zmqtt-mosquitto zmqtt-tests 10
    dynsec addRoleACL zmqtt-tests subscribePattern '#' allow 10
    dynsec addRoleACL zmqtt-tests publishClientSend '#' allow 10
    dynsec addRoleACL zmqtt-tests publishClientSend 'zmqtt/e2e/denied' deny 20
    dynsec addRoleACL zmqtt-tests unsubscribePattern '#' allow 10
    dynsec addRoleACL zmqtt-tests unsubscribePattern 'zmqtt/unsuback/denied/#' deny 20
}

has_acls() {
    acls=$(dynsec getRole "$1" | tr -s ' ')
    shift
    for acl in "$@"; do
        printf '%s\n' "$acls" | grep -qF "$acl" || return 1
    done
}

granted() {
    dynsec getAnonymousGroup | grep -q zmqtt-anonymous-clients || return 1
    dynsec getGroup zmqtt-anonymous-clients | grep -q zmqtt-anonymous-role || return 1
    dynsec getClient zmqtt-mosquitto | grep -q zmqtt-tests || return 1
    has_acls zmqtt-anonymous-role \
        'subscribePattern : allow : #' \
        'publishClientSend : allow : #' || return 1
    has_acls zmqtt-tests \
        'subscribePattern : allow : #' \
        'publishClientSend : allow : #' \
        'publishClientSend : deny : zmqtt/e2e/denied' \
        'unsubscribePattern : allow : #' \
        'unsubscribePattern : deny : zmqtt/unsuback/denied/#' || return 1
}

attempt=0
until dynsec getClient admin | grep -q '^Username:'; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        echo "Mosquitto Dynamic Security did not become ready" >&2
        exit 1
    fi
    sleep 1
done

# mosquitto_ctrl exits 0 even when the broker rejects a command, and a dropped
# response is silent, so applying the grants once proves nothing. Every command
# above is idempotent, so re-apply them until a read-back confirms all of them.
attempt=0
until grant >/dev/null && granted; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 3 ]; then
        echo "Dynamic Security grants were not applied:" >&2
        dynsec getClient zmqtt-mosquitto >&2
        dynsec getRole zmqtt-tests >&2
        exit 1
    fi
    sleep 1
done
