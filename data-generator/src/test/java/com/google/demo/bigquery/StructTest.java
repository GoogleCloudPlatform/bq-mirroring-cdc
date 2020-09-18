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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

class StructTest {
  @org.junit.jupiter.api.Test
  void addString() {
    Struct struct = new Struct();
    struct.addString("abc", "xyz");
    struct.addString("dce", "Poor man's test");
    assertEquals("STRUCT('xyz' as abc,'Poor man\\'s test' as dce)", struct.toStructConstant());
  }

  @org.junit.jupiter.api.Test
  void addTimestampInMicroseconds() {
    Struct struct = new Struct();
    struct.addTimestamp("abc", Instant.ofEpochSecond( 1571068536L, 842*1000_000));
    assertEquals("STRUCT(TIMESTAMP('2019-10-14 15:55:36.842000+00:00') as abc)", struct.toStructConstant());
  }
}