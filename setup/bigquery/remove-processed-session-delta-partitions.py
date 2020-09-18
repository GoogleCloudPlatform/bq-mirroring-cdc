#  Copyright 2020 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from google.cloud import bigquery

def remove_partitions():
  client = bigquery.Client()

  query_file = open('get-session-delta-partitions-safe-to-delete.sql', "r")
  query = query_file.read()
  query_job = client.query(query)

  results = query_job.result()

  for row in results:
    partition_id = row.partition_id
    table_ref = client.dataset('cdc_demo').table("{}${}".format('session_delta', partition_id))
    print("Partition to be deleted: {}".format(table_ref))
    client.delete_table(table_ref)

if __name__ == '__main__':
  remove_partitions()