#!/bin/sh

docker run --rm -it \
       -p 8080:8080 \
       -p 9080:9080 \
       -p 8000:8000 \
       dgraph/standalone:v20.03.0
