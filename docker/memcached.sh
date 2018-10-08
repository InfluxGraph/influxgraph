#!/bin/sh
# `/sbin/setuser memcache` runs the given command as the user `memcache`.
# If you omit that part, the command will be run as root.

#read conf file
options=$(cat /etc/memcached.conf | grep -v '#' | grep -v '^$' | tr '\n' ' ')

exec /usr/bin/memcached $options >>/var/log/memcached.log 2>&1
