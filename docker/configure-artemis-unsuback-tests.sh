#!/bin/sh
set -eu

broker_xml=${1:-/var/lib/artemis-instance/etc/broker.xml}
marker='zmqtt.unsuback.denied.#'

attempt=0
while [ ! -f "$broker_xml" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge 30 ]; then
        echo "Artemis broker.xml was not created: $broker_xml" >&2
        exit 1
    fi
    sleep 1
done

if grep -Fq "$marker" "$broker_xml"; then
    exit 0
fi

tmp_file=$(mktemp)
awk '
    /<security-settings>/ && !inserted {
        print
        print "         <security-setting match=\"zmqtt.unsuback.denied.#\">"
        print "            <permission type=\"createDurableQueue\" roles=\"amq\"/>"
        print "            <permission type=\"createNonDurableQueue\" roles=\"amq\"/>"
        print "            <permission type=\"createAddress\" roles=\"amq\"/>"
        print "            <permission type=\"consume\" roles=\"amq\"/>"
        print "            <permission type=\"send\" roles=\"amq\"/>"
        print "            <permission type=\"deleteDurableQueue\" roles=\"unsuback_test_nobody\"/>"
        print "            <permission type=\"deleteNonDurableQueue\" roles=\"unsuback_test_nobody\"/>"
        print "         </security-setting>"
        inserted=1
        next
    }
    { print }
    END { if (!inserted) exit 1 }
' "$broker_xml" > "$tmp_file"
cat "$tmp_file" > "$broker_xml"
rm "$tmp_file"
