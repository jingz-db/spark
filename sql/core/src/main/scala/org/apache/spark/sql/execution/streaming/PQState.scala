/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore}
import org.apache.spark.sql.types.{BinaryType, DataType, NullType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

object PQStateSchema {
  val PQ_KEY_ROW_SCHEMA: StructType = new StructType()
    .add("grouping_key", BinaryType)
    .add("uuid", StringType)

  val PQ_VALUE_ROW_SCHEMA: StructType =
    StructType(Array(StructField("__dummy__", NullType)))
}

case class PQKeyRow(
    groupingKey: Array[Byte],
    uuid: Long)
class PQStateImpl(stateName: String, store: StateStore) {
  import PQStateSchema._

  private val pqColumnFamilyName = s"_pq_$stateName"

  private val pqKeyRowEncoder = UnsafeProjection.create(PQ_KEY_ROW_SCHEMA)

  // empty row used for values
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  store.createColFamilyIfAbsent(pqColumnFamilyName, PQ_KEY_ROW_SCHEMA, PQ_VALUE_ROW_SCHEMA,
    NoPrefixKeyStateEncoderSpec(PQ_KEY_ROW_SCHEMA), isInternal = true)

  def exists(): Boolean = {
    val iterator = store.iterator(pqColumnFamilyName)
    iterator.hasNext
  }
  def clearPQState(): Unit = {
    val iterator = store.iterator(pqColumnFamilyName)
    iterator.foreach { kv =>
      store.remove(kv.key, pqColumnFamilyName)
    }
  }

  def removePQForStateKey(groupingKey: Array[Byte], uuid: String): Unit = {
    val encodedPqKey = pqKeyRowEncoder(InternalRow(groupingKey, UTF8String.fromString(uuid)))
    store.remove(encodedPqKey, pqColumnFamilyName)
  }

  def upsertPQForStateKey(
      groupingKey: Array[Byte],
      uuid: String): Unit = {
    val encodedPqKey = pqKeyRowEncoder(InternalRow(groupingKey, UTF8String.fromString(uuid)))
    store.put(encodedPqKey, EMPTY_ROW, pqColumnFamilyName)
  }
}
