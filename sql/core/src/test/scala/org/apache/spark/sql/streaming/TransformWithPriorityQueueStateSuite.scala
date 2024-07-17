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

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf

case class PQInputEvent(
    key: String,
    value: Int)
class PriorityQueueStatefulProcessor
  extends StatefulProcessor[String, PQInputEvent, Int] {
  @transient private var _pq: PriorityQueueState[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _pq = getHandle
      .getPriorityQueueState("valueState", Encoders.scalaInt, (i: Int) => i.toLong)
      .asInstanceOf[PriorityQueueState[Int]]
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[PQInputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[Int] = {
    inputRows.foreach { inputRow =>
      _pq.offer(inputRow.value)
    }
    _pq.get()
  }
}
class TransformWithPriorityQueueStateSuite extends StreamTest {

  import testImplicits._

  test("test priority queue") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {

      val inputData = MemoryStream[PQInputEvent]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new PriorityQueueStatefulProcessor(),
          TimeMode.None(),
          OutputMode.Update())

      testStream(result, OutputMode.Update()) (
        AddData(inputData,
          PQInputEvent("a", 3),
          PQInputEvent("a", 2),
          PQInputEvent("a", 1)),
        CheckNewAnswer(1, 2, 3)
      )
    }
  }
}
