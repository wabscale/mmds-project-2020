#!/bin/sh

cd $(dirname $(realpath $0))

if [ ! -d node_modules ]; then
    npm i
fi

for mmdf in $(find -name '*.mmd'); do
    echo "rendering ${mmdf}"
    npx mmdc -i ${mmdf} -o ../${mmdf}.svg -t forest
done

