/**
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

resource "google_bigtable_instance" "instance" {
  name = var.bigtable_instance_id
  cluster {
    cluster_id = "bq-sync-instance-cluster"
    zone = "us-central1-b"
    storage_type = "HDD"
    num_nodes = 1
  }
  deletion_protection = false
}

resource "google_bigtable_table" "session" {
  name = "session"
  instance_name = google_bigtable_instance.instance.name
  column_family {
    family = "main"
  }
}
