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

SELECT *, CURRENT_TIMESTAMP() as measured_ts FROM
(SELECT
  'Records not in destination' AS description,
  COUNT(*) AS count
FROM
  cdc_demo.session_source_v src
WHERE
  NOT EXISTS(
  SELECT
    session_id
  FROM
    cdc_demo.session_main dest
  WHERE
    dest.session_id = src.session_id)
UNION ALL
SELECT
  'Records not in source' AS description,
  COUNT(*) AS count
FROM
  cdc_demo.session_main dest
WHERE
  NOT EXISTS(
  SELECT
    session_id
  FROM
    cdc_demo.session_source_v src
  WHERE
    dest.session_id = src.session_id)
UNION ALL
SELECT
  'Records with data mismatch' AS description,
  COUNT(*) AS count
FROM
  cdc_demo.session_source_v src
INNER JOIN
  cdc_demo.session_main dest
ON
  dest.session_id = src.session_id
  WHERE dest.status <> src.status
  UNION ALL
  SELECT
    'Total records in the source' AS description,
    COUNT(*) AS count
  FROM
    cdc_demo.session_source_v src ) ORDER BY 1