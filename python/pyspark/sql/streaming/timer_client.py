#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import Any, Union, cast, Tuple

from pyspark.sql.streaming.stateful_processor_api_client import StatefulProcessorApiClient
from pyspark.errors import PySparkRuntimeError

__all__ = ["TimerValueClient", "ExpiryTimerInfoClient"]


class TimerValueClient:
    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client

    def get_processing_time_in_ms(self) -> int:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_processing_time_call = stateMessage.GetProcessingTime()
        timer_value_call = stateMessage.TimerValueRequest(getProcessingTimer=get_processing_time_call)
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting processing timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            # TODO: can we simply parse from utf8 string here?
            timestamp = int(response_message[2])
            return timestamp

    def get_watermark_in_ms(self) -> int:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_watermark_call = stateMessage.GetWatermark()
        timer_value_call = stateMessage.TimerValueRequest(getWatermark=get_watermark_call)
        timer_request = stateMessage.TimerRequest(timerValueRequest=timer_value_call)
        message = stateMessage.StateRequest(timerRequest=timer_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting processing timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            # TODO: can we simply parse from utf8 string here?
            timestamp = int(response_message[2])
            return timestamp


class ExpiryTimerInfoClient:

    def __init__(self, stateful_processor_api_client: StatefulProcessorApiClient) -> None:
        self._stateful_processor_api_client = stateful_processor_api_client

    def is_valid(self) -> bool:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        is_valid_call = stateMessage.IsValid()
        expiry_info_call = stateMessage.ExpiryTimerInfoRequest(IsValid=is_valid_call)
        expiry_info_request = stateMessage.TimerRequest(expiryTimerInfoRequest=expiry_info_call)
        message = stateMessage.StateRequest(timerRequest=expiry_info_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting processing timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            # TODO: can we simply parse from utf8 string here?
            return str(response_message[2]) == "True"

    def get_expiry_time_in_ms(self) -> int:
        import pyspark.sql.streaming.StateMessage_pb2 as stateMessage

        get_expiry_time_call = stateMessage.getExpiryTime()
        expiry_info_call = stateMessage.ExpiryTimerInfoRequest(getExpiryTime=get_expiry_time_call)
        expiry_info_request = stateMessage.TimerRequest(expiryTimerInfoRequest=expiry_info_call)
        message = stateMessage.StateRequest(timerRequest=expiry_info_request)

        self._stateful_processor_api_client._send_proto_message(message.SerializeToString())
        response_message = self._stateful_processor_api_client._receive_proto_message()
        status = response_message[0]
        if status != 0:
            # TODO(SPARK-49233): Classify user facing errors.
            raise PySparkRuntimeError(f"Error getting processing timestamp: " f"{response_message[1]}")
        else:
            if len(response_message[2]) == 0:
                return -1
            # TODO: can we simply parse from utf8 string here?
            timestamp = int(response_message[2])
            return timestamp