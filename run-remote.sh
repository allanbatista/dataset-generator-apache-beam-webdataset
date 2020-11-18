#!/usr/bin/env bash
# https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

NAME=dataset-generator
JOB_ID=${NAME}-$(date +%Y%d%m%H%M%S)
BASE_PATH=gs://allanbatista-us-east1-expire/dataflow/${NAME}/$(date +%Y/%d/%m)/${JOB_ID}
OUTPUT_PATH=${BASE_PATH}/output
GCP_TEMP_PATH=${BASE_PATH}/gcp_tmp
TEMP_PATH=${BASE_PATH}/tmp
STG_PATH=${BASE_PATH}/stg

NUM_WORKER=1
MACHINE_TYPE=e2-standard-4
TRAIN_SHARD=1
VALID_SHARD=1
TEST_SHARD=1

mvn clean && \
mvn compile exec:java \
    -Dexec.mainClass=br.com.allanbatista.dataset_generator.Main \
    -Dexec.args="--runner=DataflowRunner --project=allanbatista --region=us-east1 --stagingLocation=${STG_PATH} --tempLocation=${TEMP_PATH} --gcpTempLocation=${GCP_TEMP_PATH} --outputDir=${OUTPUT_PATH} --numWorkers=${NUM_WORKER} --maxNumWorkers=${NUM_WORKER}  --autoscalingAlgorithm=NONE --workerMachineType=${MACHINE_TYPE} --diskSizeGb=50 --trainShards=${TRAIN_SHARD} --validShards=${VALID_SHARD} --testShards=${TEST_SHARD}"
