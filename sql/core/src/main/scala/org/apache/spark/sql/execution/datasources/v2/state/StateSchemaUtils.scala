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
package org.apache.spark.sql.execution.datasources.v2.state

import scala.collection.mutable

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.StateVariableType._
import org.apache.spark.sql.execution.streaming.TransformWithStateVariableInfo
import org.apache.spark.sql.execution.streaming.state.{StateStoreColFamilySchema, UnsafeRowPair}
import org.apache.spark.sql.types.{IntegerType, LongType, MapType, StringType, StructType}

/**
 * Utility functions to generate and process schema for state store data source.
 */
object StateSchemaUtils {

  private def generateSchemaForStateVar(
      stateVarInfo: TransformWithStateVariableInfo,
      stateStoreColFamilySchema: StateStoreColFamilySchema): StructType = {
    val stateVarType = stateVarInfo.stateVariableType
    val hasTTLEnabled = stateVarInfo.ttlEnabled

    stateVarType match {
      case ValueState =>
        if (hasTTLEnabled) {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("expiration_timestamp", LongType)
            .add("partition_id", IntegerType)
        } else {
          new StructType()
            .add("key", stateStoreColFamilySchema.keySchema)
            .add("value", stateStoreColFamilySchema.valueSchema)
            .add("partition_id", IntegerType)
        }

      case MapState =>
        val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
          stateStoreColFamilySchema.keySchema, "key")
        val userKeySchema = stateStoreColFamilySchema.userKeyEncoderSchema.get
        val valueMapSchema = new MapType(
          keyType = userKeySchema,
          valueType = stateStoreColFamilySchema.valueSchema,
          valueContainsNull = false
        )

        new StructType()
          .add("key", groupingKeySchema)
          .add("value", valueMapSchema)
          .add("partition_id", IntegerType)

      case _ =>
        throw StateDataSourceErrors.internalError(s"Unsupported state variable type $stateVarType")
    }
  }

  def getSourceSchema(
      sourceOptions: StateSourceOptions,
      keySchema: StructType,
      valueSchema: StructType,
      transformWithStateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema]): StructType = {
    if (sourceOptions.readChangeFeed) {
      new StructType()
        .add("batch_id", LongType)
        .add("change_type", StringType)
        .add("key", keySchema)
        .add("value", valueSchema)
        .add("partition_id", IntegerType)
    } else if (transformWithStateVariableInfoOpt.isDefined) {
      require(stateStoreColFamilySchemaOpt.isDefined)
      generateSchemaForStateVar(transformWithStateVariableInfoOpt.get,
        stateStoreColFamilySchemaOpt.get)
    } else {
      new StructType()
        .add("key", keySchema)
        .add("value", valueSchema)
        .add("partition_id", IntegerType)
    }
  }

  def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow), partition: Int): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition)
    row
  }

  def unifyStateRowPairWithTTL(
      pair: (UnsafeRow, UnsafeRow),
      valueSchema: StructType,
      partition: Int): InternalRow = {
    val row = new GenericInternalRow(4)
    row.update(0, pair._1)
    row.update(1, pair._2.get(0, valueSchema))
    row.update(2, pair._2.get(1, LongType))
    row.update(3, partition)
    row
  }

  private def getUserKeySchema(
      stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      schema: StructType): Option[StructType] = {
    if (!stateVariableInfoOpt.isDefined ||
    stateVariableInfoOpt.get.stateVariableType != MapState) {
      None
    } else {
      try {
        Option(
          SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[MapType]
            .keyType.asInstanceOf[StructType])
      } catch {
        case _: Exception =>
          None
      }
    }
  }

  def getGroupingKeySchema(
      stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      schema: StructType): StructType = {
    stateVariableInfoOpt.get.stateVariableType match {
      case ValueState =>
        SchemaUtil.getSchemaAsDataType(
          schema, "key").asInstanceOf[StructType]
      case MapState =>
        val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
          schema, "key").asInstanceOf[StructType]
        val userKeySchema = getUserKeySchema(stateVariableInfoOpt, schema)
        new StructType()
          .add("key", groupingKeySchema)
          .add("userKey", userKeySchema.get)
      case _ =>
        throw StateDataSourceErrors.internalError(
          s"Unsupported state variable type ${stateVariableInfoOpt.get.stateVariableType}")
    }
  }

  def getValueSchema(
      stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
      schema: StructType): StructType = {
    stateVariableInfoOpt.get.stateVariableType match {
      case ValueState =>
        SchemaUtil.getSchemaAsDataType(
          schema, "value").asInstanceOf[StructType]
      case MapState =>
        SchemaUtil.getSchemaAsDataType(
          schema, "value").asInstanceOf[MapType]
          .valueType.asInstanceOf[StructType]
      case _ =>
        throw StateDataSourceErrors.internalError(
          s"Unsupported state variable type ${stateVariableInfoOpt.get.stateVariableType}")
    }
  }

  def unifyMapStateRowPair(
      stateRows: Iterator[UnsafeRowPair],
      compositeKeySchema: StructType,
      partitionId: Int): Iterator[InternalRow] = {
    val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
      compositeKeySchema, "key"
    ).asInstanceOf[StructType]
    val userKeySchema = SchemaUtil.getSchemaAsDataType(
      compositeKeySchema, "userKey"
    ).asInstanceOf[StructType]

    def appendKVPairToMap(
        curMap: mutable.Map[Any, Any],
        stateRowPair: UnsafeRowPair): Unit = {
      curMap += (
        stateRowPair.key.get(1, userKeySchema)
          .asInstanceOf[UnsafeRow].copy() ->
          stateRowPair.value.copy()
        )
    }

    def updateDataRow(
        groupingKey: Any,
        curMap: mutable.Map[Any, Any]): GenericInternalRow = {
      val row = new GenericInternalRow(3)
      val mapData = new ArrayBasedMapData(
        ArrayData.toArrayData(curMap.keys.toArray),
        ArrayData.toArrayData(curMap.values.toArray)
      )
      row.update(0, groupingKey)
      row.update(1, mapData)
      row.update(2, partitionId)
      row
    }

    // state rows are sorted in rocksDB. So all of the rows with same
    // user key should be grouped together consecutively
    new Iterator[InternalRow] {
      var curGroupingKey: UnsafeRow = _
      var curStateRowPair: UnsafeRowPair = _
      val curMap = mutable.Map.empty[Any, Any]

      override def hasNext: Boolean =
        stateRows.hasNext || !curMap.isEmpty

      override def next(): InternalRow = {
        var keepGoing = true
        while (stateRows.hasNext && keepGoing) {
          curStateRowPair = stateRows.next()
          if (curGroupingKey == null) {
            // First time in the iterator
            // Need to make a copy because we need to keep the
            // value across function calls
            curGroupingKey = curStateRowPair.key
              .get(0, groupingKeySchema).asInstanceOf[UnsafeRow].copy()
            appendKVPairToMap(curMap, curStateRowPair)
          } else {
            val curPairGroupingKey =
              curStateRowPair.key.get(0, groupingKeySchema)
            if (curPairGroupingKey == curGroupingKey) {
              appendKVPairToMap(curMap, curStateRowPair)
            } else {
              // find a different grouping key, exit loop and return a row
              keepGoing = false
            }
          }
        }
        if (!keepGoing) {
          // found a different grouping key
          val row = updateDataRow(curGroupingKey, curMap)
          // update vars
          curGroupingKey =
            curStateRowPair.key.get(0, groupingKeySchema)
              .asInstanceOf[UnsafeRow].copy()
          // empty the map, append current row
          curMap.clear()
          appendKVPairToMap(curMap, curStateRowPair)
          // return map value of previous grouping key
          row
        } else {
          // reach the end of the state rows
          if (curMap.isEmpty) null.asInstanceOf[InternalRow]
          else {
            val row = updateDataRow(curGroupingKey, curMap)
            // clear the map to end the iterator
            curMap.clear()
            row
          }
        }
      }
    }
  }
}
