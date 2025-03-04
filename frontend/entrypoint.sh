#!/bin/sh

TARGETS="
FE_TYPESENSE_HOST
FE_TYPESENSE_PORT
FE_TYPESENSE_SEARCHKEY
"

for VAR in $TARGETS; do
    ENVAR=$(eval echo \$$VAR)
    sed -i "s|${VAR}|${ENVAR}|g" /usr/share/nginx/html/index.html
done

# defer
exec "$@"
