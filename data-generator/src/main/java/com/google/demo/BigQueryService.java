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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.Builder;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.demo.bigquery.Struct;
import com.google.demo.model.Session;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Helper class to encapsulate BigQuery services
 */
class BigQueryService {

  private static final Logger log = Logger.getLogger(BigQueryService.class.getName());

  private static final String DI_SEQUENCE_COLUMN = "di_sequence_number";
  private static final String DI_OPERATION_COLUMN = "di_operation_type";

  // Operation types
  private static final String INSERT_OP = "I";
  private static final String UPDATE_OP = "U";
  private static final String DELETE_OP = "D";

  private final BigQuery bigQuery;
  // Technically doesn't need to be atomic (the demo generator doesn't use concurrency at the moment)
  private final AtomicInteger insertSequence = new AtomicInteger();

  /**
   * @param bigQuery all the operations will use this object to operate on BigQuery
   */
  BigQueryService(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  /**
   * Adds a row to indicate an INSERT in the source table.
   */
  void addInsertRow(InsertAllRequest.Builder requestBuilder, Map<String, Object> row) {
    addRowWithOperation(requestBuilder, row, INSERT_OP);
  }

  /**
   * Adds a row to indicate an UPDATE in the source table.
   */
  void addUpdateRow(InsertAllRequest.Builder requestBuilder, Map<String, Object> row) {
    addRowWithOperation(requestBuilder, row, UPDATE_OP);
  }

  /**
   * Adds a row to indicate a DELETE in the source table.
   */
  void addDeleteRow(InsertAllRequest.Builder requestBuilder, Map<String, Object> row) {
    addRowWithOperation(requestBuilder, row, DELETE_OP);
  }

  /**
   * Helper function to add a row.
   */
  private void addRowWithOperation(Builder requestBuilder, Map<String, Object> row,
      String operation) {
    row.put(DI_OPERATION_COLUMN, operation);
    row.put(DI_SEQUENCE_COLUMN, insertSequence.incrementAndGet());

    requestBuilder.addRow(row);
  }


  /**
   * Populate BigQuery table with session data.
   *
   * NOTE: at the moment doesn't insert the data into the Bigtable
   */
  void doBatchInserts(TableId tableId,
      int recordCount, int batchSize) throws InterruptedException {

    while (recordCount > 0) {
      int nextBatchSize = Math.min(batchSize, recordCount);
      log.info("Inserting next batch of " + nextBatchSize + " records.");

      StringBuilder queryBuilder = new StringBuilder();
      queryBuilder.append("INSERT INTO `").append(tableId.getProject()).append('.')
          .append(tableId.getDataset()).append(".").append(tableId.getTable())
          .append("` SELECT * FROM UNNEST([");
      for (int i = 0; i < nextBatchSize; i++) {
        Session session = new Session();
        Struct struct = session.toBigQueryStruct();

        if (i > 0) {
          queryBuilder.append(",");
        }
        queryBuilder.append(struct.toStructConstant());
      }
      queryBuilder.append("])");

      String query = queryBuilder.toString();
      log.fine("Query: " + query);
      QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
          .build();
      bigQuery.query(queryConfig);

      recordCount -= batchSize;
    }
  }

  /**
   * Process the streaming inserts.
   *
   * @param insertRequestBuilder to run
   */
  void runInsertAll(Builder insertRequestBuilder) {
    InsertAllRequest insertRequest = insertRequestBuilder.build();
    InsertAllResponse insertResponse = bigQuery.insertAll(insertRequest);
    if (insertResponse.hasErrors()) {
      // This is an acceptable approach for a demo program
      throw new RuntimeException(
          "Errors occurred while inserting rows: " + insertResponse.getInsertErrors());
    } else {
      log.info("Inserted next batch of " + insertRequest.getRows().size() + " row(s).");
    }
  }
}
