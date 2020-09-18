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

package com.google.demo.model;

import com.google.demo.Constants;
import com.google.demo.bigquery.ConversionUtil;
import com.google.demo.bigquery.Struct;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Session model used for the demo.
 */
public class Session {

  public enum Status {NEW, ABANDONED, LOGGED_IN, LOGGED_OUT}

  private final String sessionId;
  private Status status;
  private String customerKey;
  private final Instant start;
  private Instant end;

  public Session() {
    sessionId = UUID.randomUUID().toString();
    status = Status.NEW;
    start = Instant.now();
  }

  public void loggedIn(String customerKey) {
    status = Status.LOGGED_IN;
    this.customerKey = customerKey;
  }

  public void abandon() {
    status = Status.ABANDONED;
    end = Instant.now();
  }

  public void logout() {
    status = Status.LOGGED_OUT;
    end = Instant.now();
  }

  public Map<String, Object> toBigQueryRow() {
    Map<String, Object> result = new HashMap<>();
    result.put(Constants.SESSION_ID_COLUMN, sessionId);
    result.put(Constants.STATUS_COLUMN, status.name());
    result.put(Constants.CUSTOMER_KEY_COLUMN, customerKey);
    result.put(Constants.START_COLUMN, ConversionUtil.convertToTimestamp(start));
    result.put(Constants.END_COLUMN, ConversionUtil.convertToTimestamp(end));
    return result;
  }

  public Struct toBigQueryStruct() {
    Struct result = new Struct();
    // Order of fields must match the order defined in the table definition (see bigquery.tf).
    result.addString(Constants.SESSION_ID_COLUMN, sessionId)
        .addString(Constants.STATUS_COLUMN, status.name())
        .addString(Constants.CUSTOMER_KEY_COLUMN, customerKey)
        .addTimestamp(Constants.START_COLUMN, start)
        .addTimestamp(Constants.END_COLUMN, end);
    return result;
  }

  public String getSessionId() {
    return sessionId;
  }

  public Status getStatus() {
    return status;
  }

  public String getCustomerKey() {
    return customerKey;
  }

  public Instant getStart() {
    return start;
  }

  public Instant getEnd() {
    return end;
  }
}
