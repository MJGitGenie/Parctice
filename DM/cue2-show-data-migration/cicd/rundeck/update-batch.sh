#!/usr/bin/env bash
# Script to update batch job definition and cloudwatch rule

if [[ $# -lt 4 ]]; then
    echo "Invalid Number of Parameters.  Usage: APP_ENV TAG APP_NAME PROJECT [true/false]"
    echo "Example:  sbx 1.0.0-b6b731b ftp-discovery cue2"
    echo "5th optional parameter: if true, will update corresponding CloudWatch Rule"
    exit 1
fi

#setup env
export company="ascap"
export project=$4

#todo:  image, job and name are the same value.  Need to clean it up.
export image=$3
name=$3
job=$3
here=`dirname $0`
app_env=$1
tag=$2

if [[ $# -eq 5 ]]; then
    updateCloudWatchRule=$5
fi

#load lib for other functions
source ${here}/cicd-lib.sh

job_def_arn=`read_batch_job_def_arn ${app_env}`

register_flag=1

if [[ "${job_def_arn}" == "null" ]]; then
    echo "no active job definition"
    job_def_arn=`read_batch_job_def_arn_inactive ${app_env}`
    register_flag=0
fi

echo job definition arn: ${job_def_arn}

#update the job definition in batch
update_batch ${app_env} ${tag}

#update CloudWatch Rule
if [[ "${updateCloudWatchRule}" == "true" ]]; then
    echo "update CloudWatch Rule"
    update_cloud_watch ${app_env}
fi

#Deregister batch job definition
if [[ ${register_flag} -gt 0 ]]; then
    echo "aws batch deregister-job-definition --job-definition ${job_def_arn}"
    aws batch deregister-job-definition --job-definition ${job_def_arn}
fi