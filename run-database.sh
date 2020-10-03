#!/bin/bash

CONTAINER_NAME="pgevents"

if [[ ! "$(docker ps -aqf name=$CONTAINER_NAME)" ]]; then
  docker run \
    -e 'POSTGRES_USER=postgres' \
    -e 'POSTGRES_PASSWORD=development' \
    -e 'POSTGRES_DB=postgres' \
    -p 5432:5432 \
    -d \
    --name "$CONTAINER_NAME" \
    postgres:11-alpine
else
  docker start "$CONTAINER_NAME"
fi

echo "started container -> $CONTAINER_NAME"
