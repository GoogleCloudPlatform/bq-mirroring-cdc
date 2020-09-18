#!/bin/bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -e

PROJECT_ID=$1
BQ_DATASET=$2
FINAL_DEF_FILE=session-def-final.json

cat session-def.json | sed s/PROJECT_ID/${PROJECT_ID}/ > ${FINAL_DEF_FILE}

bq mk --external_table_definition=${FINAL_DEF_FILE} ${BQ_DATASET}.source_session

rm ${FINAL_DEF_FILE}
