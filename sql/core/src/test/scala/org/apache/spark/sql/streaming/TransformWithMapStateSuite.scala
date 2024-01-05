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

package org.apache.spark.sql.streaming

// import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class InputMapRow(key: String, action: String, value: (String, Int))

class TestMapStateProcessor
  extends StatefulProcessor[String, InputMapRow, (String, String, Int)] {

  @transient var _processorHandle: StatefulProcessorHandle = _
  @transient var _mapState: MapState[String, Int] = _

  override def init(handle: StatefulProcessorHandle, outputMode: OutputMode): Unit = {
    _processorHandle = handle
    _mapState = handle.getMapState("sessionState")
  }

  override def handleInputRows(key: String,
                               inputRows: Iterator[InputMapRow],
                               timerValues: TimerValues): Iterator[(String, String, Int)] = {

    var output = List[(String, String, Int)]()

    for (row <- inputRows) {
      if (row.action == "emit") {
        output = (key, row.value._1, row.value._2) :: output
      } else if (row.action == "getValue") {
        output = (key, row.value._1, _mapState.getValue(row.value._1)) :: output
      } else if (row.action == "updateValue") {
        _mapState.updateValue(row.value._1, row.value._2)
      } else if (row.action == "getMap") {
        val res = _mapState.getMap()
          res.foreach { pair =>
            output = (key, pair._1, pair._2) :: output
          }
      } else if (row.action == "remove") {
        _mapState.remove()
      } else if (row.action == "getKeys") {
        _mapState.getKeys().foreach { key =>
          output = (row.key, key, row.value._2) :: output
        }
      } else if (row.action == "getVals") {
        _mapState.getValues().foreach { value =>
          output = (row.key, row.value._1, value) :: output
        }
      }
    }
    output.iterator
  }


  override def close(): Unit = {
  }
}

class TransformWithMapStateSuite extends StreamTest {
  import testImplicits._

  test("test list state correctness") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val inputData = MemoryStream[InputMapRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new TestMapStateProcessor(),
          TimeoutMode.noTimeouts(),
          OutputMode.Append())
      testStream(result, OutputMode.Append())(
        AddData(inputData, InputMapRow("k1", "updateValue", ("v1", 10))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", 3))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v2", 12))),
        AddData(inputData, InputMapRow("k2", "updateValue", ("v4", 1))),

        AddData(inputData, InputMapRow("k2", "getMap", ("", -1))),
        CheckNewAnswer(("k2", "v2", 12), ("k2", "v4", 1)),

        AddData(inputData, InputMapRow("k2", "getKeys", ("", -1))),
        CheckNewAnswer(("k2", "v2", -1), ("k2", "v4", -1)),

        AddData(inputData, InputMapRow("k2", "getVals", ("", -1))),
        CheckNewAnswer(("k2", "", 12), ("k2", "", 1)),

        AddData(inputData, InputMapRow("k2", "remove", ("", -1))),
        AddData(inputData, InputMapRow("k2", "getMap", ("", -1))),
        CheckNewAnswer()


      )

    }
  }
}
