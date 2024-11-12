function load_event_rule_name()
{
    # input app_env
    aws events describe-rule --name ${project}-${name}-${1}-${company} | jq --raw-output '.Name'
}

function load_batch_job_def_name()
{
    # input app_env
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} --status ACTIVE \
    --query jobDefinitions[*].jobDefinitionName | jq --raw-output '.[0]'
}

function load_ecr_image_name()
{
    # input app_env
    aws ecr describe-images --repository-name ${project}-${image}-${1}-${company} | jq --raw-output '.imageDetails[0].repositoryName'
}

function read_event_target()
{
    # input rule_name
    aws events list-targets-by-rule --rule $1 | jq --raw-output '.Targets[0]'
}

function read_batch_job_def_arn()
{
    # input app_env
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} --status ACTIVE \
    --query jobDefinitions[*].jobDefinitionArn | jq 'sort_by(.) | reverse' | jq --raw-output '.[0]'
}

function read_batch_job_def_arn_inactive()
{
    # input app_env
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} --status INACTIVE \
    --query jobDefinitions[*].jobDefinitionArn | jq 'sort_by(.) | reverse' | jq --raw-output '.[0]'
}

function read_batch_job_def_version()
{
    # input app_env
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} --status ACTIVE \
    --query jobDefinitions[*].revision | jq 'sort_by(.) | reverse' | jq --raw-output '.[0]'
}

function read_batch_job_def_version_inactive()
{
    # input app_env
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} --status INACTIVE \
    --query jobDefinitions[*].revision | jq 'sort_by(.) | reverse' | jq --raw-output '.[0]'
}

function read_event_job_def_arn()
{
    # input rule_name
    read_event_target $1 | jq --raw-output '.BatchParameters.JobDefinition'
}

function make_temp_dir()
{
    temp=`mktemp`
    rm -rf ${temp}
    mkdir -p ${temp}
    echo ${temp}
}

function update_batch()
{
    # input app_env tag
    job_def_version=`read_batch_job_def_version ${1}`
    ecr_image_name=`load_ecr_image_name ${1}`

    if [[ ${job_def_version} -eq "null" ]]; then
        job_def_version=`read_batch_job_def_version_inactive ${1}`
    fi

    if [[ ${ecr_image_name} -eq "null" ]]; then
        ecr_image_name=${project}-${image}-${1}-${company}
    fi

    echo "job_def_version: ${job_def_version}"
    echo "ecr_image_name: ${ecr_image_name}"

    temp_dir=`make_temp_dir`

    # don't mess with the back ticks!
    aws batch describe-job-definitions --job-definition-name ${project}-${name}-${1}-${company} \
    --query jobDefinitions[?revision=="\`${job_def_version}\`"] | jq '.[0]' | jq 'del(.jobDefinitionArn, .revision, .status)' \
    > ${temp_dir}/batch-job-def-1.json

    old_ecr_image=`jq --raw-output '.containerProperties.image' ${temp_dir}/batch-job-def-1.json`
    IFS='/' read -r -a array <<< "$old_ecr_image"
    new_ecr_image="${array[0]}/${ecr_image_name}:${2}"

    echo "old_ecr_image: ${old_ecr_image}"
    echo "new_ecr_image: ${new_ecr_image}"

    sed "s@${old_ecr_image}@${new_ecr_image}@" ${temp_dir}/batch-job-def-1.json > ${temp_dir}/batch-job-def-2.json

    aws batch register-job-definition --job-definition-name ${project}-${name}-${1}-${company} --type container \
    --cli-input-json file://${temp_dir}/batch-job-def-2.json

    rm -rf ${temp_dir}
}

function update_cloud_watch()
{
    # input app_env
    rule_name=`load_event_rule_name ${1}`
    target=`read_event_target ${rule_name}`
    old_job_def_arn=`read_event_job_def_arn ${rule_name}`
    new_job_def_arn=`read_batch_job_def_arn ${1}`

    temp_dir=`make_temp_dir`

    echo "rule_name: ${rule_name}"
    echo "old_job_def_arn: ${old_job_def_arn}"
    echo "new_job_def_arn: ${new_job_def_arn}"

    # TODO: only write one event-target.json file
    echo ${target} > ${temp_dir}/event-target-1.json
    sed "s@${old_job_def_arn}@${new_job_def_arn}@" ${temp_dir}/event-target-1.json > ${temp_dir}/event-target-2.json

    # create event-rule.json for update
    cat > ${temp_dir}/event-rule.json <<EOF
{
    "Rule": "${rule_name}",
    "Targets": [
        `jq '' ${temp_dir}/event-target-2.json`
    ]
}
EOF

    aws events put-targets --cli-input-json file://${temp_dir}/event-rule.json
    rm -rf ${temp_dir}
}

function _list_ecr_image_tags()
{
  # input app_env next_token
  repository_name="${project}-${image}-${1}-${company}"

  if [[ ${2} -eq "null" ]]; then
    result=`aws ecr list-images --repository-name=${repository_name} --filter tagStatus=TAGGED --max-items=10`
  else
    result=`aws ecr list-images --repository-name=${repository_name} --filter tagStatus=TAGGED --max-items=10 --starting-token ${2}`
  fi
  tags=`echo ${result} | jq '.imageIds[].imageTag' | jq --raw-output --slurp '.'`
  token=`echo ${result} | jq --raw-output '.NextToken'`
  cat <<EOF
{
  "tags": ${tags},
  "token": "${token}"
}
EOF
}

function list_ecr_image_tags()
{
  # input app_env
  # TODO: sort by date pushed
  token=null
  count=0
  while true; do
    result=`_list_ecr_image_tags ${1} ${token}`
    tags=`echo ${result} | jq --raw-output '.tags'`
    token=`echo ${result} | jq --raw-output '.token'`
    count=${count}+1
    echo ${tags} | jq --raw-output '.[]'
    if [[ ${token} == null ]]; then
        break
    fi
  done
}
