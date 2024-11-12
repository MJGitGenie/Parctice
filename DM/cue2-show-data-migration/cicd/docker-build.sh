#!/usr/bin/env bash

here=`dirname $0`
source ${here}/env.sh
export name=${image}

if [[ $# -lt 1 ]]
then
    echo "missing mode parameter [build | push]"
    exit 1
fi

if [[ $1 = "build" ]]
then
    if [[ $# -eq 2 ]]
    then
        version=$2
    else
        version="latest"
    fi
    docker build --no-cache -t ${project}-${name}:${version} ${here}/..
fi

