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

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.PriorityQueueState
import org.apache.spark.sql.types.{DataType, NullType}

class PriorityQueueStateImpl[S](
    store: StateStore,
    stateName: String,
    keyExprEnc: ExpressionEncoder[Any],
    valEncoder: Encoder[S])
  extends PriorityQueueState[S] with Logging {

  private val stateTypesEncoder = new PQKeyStateEncoder(
    keyExprEnc,
    valEncoder,
    stateName
  )

  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  private val pqKeySchema = TransformWithStateKeyValueRowSchema
    .getPqKeySchema(keyExprEnc.schema, valEncoder.schema)
  // dummy row
  private val pqValueSchema = PQStateSchema.PQ_VALUE_ROW_SCHEMA

  store.createColFamilyIfAbsent(stateName, pqKeySchema, pqValueSchema,
    RangeKeyScanStateEncoderSpec(pqKeySchema, Seq(0)))

  /** Whether state exists or not. */
  override def exists(): Boolean = {
    store.iterator(stateName).hasNext
  }

  /** Get the state value. An empty iterator is returned if no value exists. */
  override def get(): Iterator[S] = {
    val iterator = store.iterator(stateName)
    iterator.map { kv =>
      // we are decoding the key because it has the state value
      // the value is a placeholder
      stateTypesEncoder.decodeValue(kv.key)
    }
  }

  override def poll(): Option[S] = {
    val iterator = store.iterator(stateName)
    if (iterator.hasNext) {
      val kv = iterator.next()
      store.remove(kv.key, stateName)
      Some(stateTypesEncoder.decodeValue(kv.key))
    } else {
      None
    }
  }

  /** Append an entry to the list */
  override def offer(newState: S, priority: Long): Unit = {
    val uuid = UUID.randomUUID().toString
    val keyRow = stateTypesEncoder.encodeCompositeKey(priority, newState, uuid)
    store.put(keyRow, EMPTY_ROW, stateName)
  }

  /** Removes this state for the given grouping key. */
  override def clear(): Unit = {

  }
}
