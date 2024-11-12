#!/usr/bin/env bash

if [[ $# -lt 3 ]]; then
    echo "invalid usage: APP_ENV TAG JOB"
    exit 1
fi

here=`dirname $0`
app_env=$1
tag=$2
job=$3

source ${here}/cicd-lib.sh
source ${here}/env.sh
name=${job}

job_def_arn=`read_batch_job_def_arn ${app_env}`

register_flag=1

if [[ "${job_def_arn}" == "null" ]]; then
    echo "no active job definition"
    job_def_arn=`read_batch_job_def_arn_inactive ${app_env}`
    register_flag=0
fi

echo job definition arn: ${job_def_arn}

update_batch ${app_env} ${tag}
update_cloud_watch ${app_env}

if [[ ${register_flag} -gt 0 ]]; then
    echo "aws batch deregister-job-definition --job-definition ${job_def_arn}"
    aws batch deregister-job-definition --job-definition ${job_def_arn}
fi