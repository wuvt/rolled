#!/bin/sh

TARGETS="
FE_TYPESENSE_HOST
FE_TYPESENSE_PORT
FE_TYPESENSE_SEARCHKEY
FE_TYPESENSE_PROTO
"

for VAR in $TARGETS; do
    ENVAR=$(eval echo \$$VAR)
    sed -i "s|${VAR}|${ENVAR}|g" /usr/share/nginx/html/index.html
done

for ALLOWED in $FE_ALLOWED; do
    echo -n "allow ${ALLOWED};" >> /allow-list
done

ALLOW_LIST=`cat /allow-list`
sed -i "s|ALLOW_LIST|${ALLOW_LIST}|g" /etc/nginx/conf.d/default.conf

# defer
exec "$@"
