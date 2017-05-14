#!/bin/sh
# Include nginx defaults if available
if [ -r /etc/default/nginx ]; then
        . /etc/default/nginx
fi
DAEMON=/usr/sbin/nginx
$DAEMON > /dev/null
