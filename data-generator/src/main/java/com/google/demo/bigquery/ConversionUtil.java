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

package com.google.demo.bigquery;

import com.google.cloud.bigquery.QueryParameterValue;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * BigQuery data type conversion utility.
 */
public class ConversionUtil {

  /**
   * Converts instant into BigQuery timestamp
   * @param instant to convert to String
   * @return String representation of the instant
   */
  public static String convertToTimestamp(Instant instant) {
    if (instant == null) {
      return null;
    }
    return QueryParameterValue.timestamp(ChronoUnit.MICROS.between(Instant.EPOCH, instant))
        .getValue();
  }
}
