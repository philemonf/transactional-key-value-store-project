#!/bin/sh

echo "Looking for a free TCP port..."

# FIND PORT
PORT=-1
for i in $(seq 1025 65535)
do
    TMP=$(lsof -iTCP:$i)

    if [ -z $TMP ]
    then
        PORT=$i
        echo "TCP port $i is free and will be used."
        break
    fi
done

if [ $PORT -eq -1 ]
then
    echo "No port found."
    exit 1

fi



HOSTNAME=$(hostname)
echo "$HOSTNAME:$PORT" > config/slaves
