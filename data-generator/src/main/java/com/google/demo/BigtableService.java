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

package com.google.demo;

import static com.google.demo.Constants.CUSTOMER_KEY_COLUMN;
import static com.google.demo.Constants.END_COLUMN;
import static com.google.demo.Constants.START_COLUMN;
import static com.google.demo.Constants.STATUS_COLUMN;
import static com.google.demo.Constants.MAIN_FAMILY;

import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.demo.bigquery.ConversionUtil;
import com.google.demo.model.Session;

/**
 * Bigtable services
 */
class BigtableService {

  private final BigtableDataClient bigtableClient;

  /**
   * Constructs the service.
   * @param bigtableDataClient will be used to perform the operations.
   */
  BigtableService(BigtableDataClient bigtableDataClient) {
    this.bigtableClient = bigtableDataClient;
  }

  /**
   * @return <code>BulkMutation</code> object for "session" table.
   */
  BulkMutation createBulkMutationForSession() {
    return BulkMutation.create("session");
  }

  /**
   * Adds mutations required to persist Session object to Bigtable
   *
   * @param bulkMutation batch container
   * @param session to persist
   */
  void addOrUpdateSession(BulkMutation bulkMutation, Session session) {
    Mutation mutation = Mutation.create();
    setNonNullCell(mutation, MAIN_FAMILY, STATUS_COLUMN, session.getStatus().name());
    setNonNullCell(mutation, MAIN_FAMILY, CUSTOMER_KEY_COLUMN, session.getCustomerKey());
    setNonNullCell(mutation, MAIN_FAMILY, START_COLUMN,
        ConversionUtil.convertToTimestamp(session.getStart()));
    setNonNullCell(mutation, MAIN_FAMILY, END_COLUMN,
        ConversionUtil.convertToTimestamp(session.getEnd()));

    bulkMutation.add(session.getSessionId(), mutation);
  }

  /**
   * Helper function to set up a cell on a mutation.
   * If the <code>value</code> is NULL the cell is not set.
   *
   * @param mutation to update
   * @param familyName cell's family name
   * @param columnName cell's column name
   * @param value to set
   */
  private static void setNonNullCell(Mutation mutation, String familyName,
      String columnName, String value) {
    if (value == null) {
      return;
    }
    mutation.setCell(familyName, columnName, value);
  }

  /**
   * Helper function to delete a row corresponding to the session.
   *
   * @param bulkMutation to add to
   * @param session to delete
   */
  public void deleteSession(BulkMutation bulkMutation, Session session) {
    bulkMutation.add(session.getSessionId(), Mutation.create().deleteRow());
  }

  /**
   * Helper function to execute the bulk mutation.
   *
   * @param bulkMutation to run
   */
  public void bulkUpdate(BulkMutation bulkMutation) {
    bigtableClient.bulkMutateRows(bulkMutation);
  }
}
