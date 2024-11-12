#!/usr/bin/env bash

here=`dirname $0`
source ${here}/env.sh
export name=${image}

if [[ $# -gt 0 ]]
then
    filter=$1
else
    filter="${project}-${name}"
fi

echo "docker images -a | grep ${filter}"
docker images -a | grep ${filter}

echo "docker images -a | grep ${filter} | wc -l | awk '{print \$1}'"
count=`docker images -a | grep ${filter} | wc -l | awk '{print $1}'`
echo "count = ${count}"

if [[ ${count} -gt 0 ]]
then
    echo "cleaning"
    echo "docker images -a | grep ${filter} | awk '{print \$3}' | xargs docker rmi -f"
    docker images -a | grep ${filter} | awk '{print $3}' | xargs docker rmi -f
else
    echo "nothing to clean"
fi

exit 0
