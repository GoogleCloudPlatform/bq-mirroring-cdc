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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

/**
 * BigQuery helper class used to generate insert statements.
 */
public class Struct {

  private final LinkedHashMap<String, String> fields = new LinkedHashMap<>();

  public Struct addString(String name, String value) {
    if (value != null) {
      fields.put(name, stringLiteral(value));
    }
    return this;
  }

  public Struct addTimestamp(String name, Instant instant) {
    if (instant != null) {
      fields.put(name, "TIMESTAMP('" + ConversionUtil.convertToTimestamp(instant) + "')");
    }
    return this;
  }

  private static String stringLiteral(String value) {
    if (value == null) {
      return "NULL";
    }
    // TODO: too simplistic. Works only for strings without line breaks and other complexities.
    // See: https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string-and-bytes-literals
    return "'" + value.replace("'", "\\'") + "'";
  }

  public String toStructConstant() {
    StringBuilder result = new StringBuilder();
    result.append("STRUCT(");
    boolean firstField = true;
    for (Entry<String, String> entry : fields.entrySet()) {
      if (!firstField) {
        result.append(',');
      }
      result.append(entry.getValue()).append(" as ").append(entry.getKey());
      firstField = false;
    }
    result.append(")");

    return result.toString();
  }
}
