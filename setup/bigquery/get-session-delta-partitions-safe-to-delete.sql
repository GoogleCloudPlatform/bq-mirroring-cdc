/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

SELECT
  FORMAT_TIMESTAMP("%Y%m%d", pt) AS partition_id
FROM (
  SELECT
    pt,
    ROW_NUMBER() OVER (ORDER BY pt DESC) AS row_num
  FROM (
    SELECT
      DISTINCT(_partitiontime) AS pt
    FROM
      cdc_demo.session_delta d,
      cdc_demo.session_main s
    WHERE
      d.session_id = s.session_id
      AND d._partitiontime IS NOT NULL))
WHERE
  row_num > 1
ORDER BY
  partition_id