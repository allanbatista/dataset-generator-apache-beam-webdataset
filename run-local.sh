#!/usr/bin/env bash

BASE_PATH=/tmp/dataset-generator/$(date +%Y-%d-%m_%H-%M-%S)
OUTPUT_PATH=${BASE_PATH}/output
SINGLE_DATASET=true
LIMIT=100

mvn compile exec:java -X \
    -Dexec.mainClass=br.com.allanbatista.dataset_generator.Main \
    -Dexec.args="--project=allanbatista --tempLocation=gs://allanbatista-us-east1-expire/temp --outputDir=${OUTPUT_PATH} --singleDataset=${SINGLE_DATASET} --limit=${LIMIT}" \
    -Pdirect-runner

echo $OUTPUT_PATH