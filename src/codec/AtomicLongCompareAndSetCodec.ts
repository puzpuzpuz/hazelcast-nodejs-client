/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* tslint:disable */
import {ClientInputMessage, ClientOutputMessage} from '../ClientMessage';
import {BitsUtil} from '../BitsUtil';
import {Data} from '../serialization/Data';
import {AtomicLongMessageType} from './AtomicLongMessageType';

var REQUEST_TYPE = AtomicLongMessageType.ATOMICLONG_COMPAREANDSET;
var RESPONSE_TYPE = 101;
var RETRYABLE = false;


export class AtomicLongCompareAndSetCodec {


    static calculateSize(name: string, expected: any, updated: any) {
// Calculates the request payload size
        var dataSize: number = 0;
        dataSize += BitsUtil.calculateSizeString(name);
        dataSize += BitsUtil.LONG_SIZE_IN_BYTES;
        dataSize += BitsUtil.LONG_SIZE_IN_BYTES;
        return dataSize;
    }

    static encodeRequest(name: string, expected: any, updated: any) {
// Encode request into clientMessage
        var clientMessage = ClientOutputMessage.newClientMessage(this.calculateSize(name, expected, updated));
        clientMessage.setMessageType(REQUEST_TYPE);
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.appendString(name);
        clientMessage.appendLong(expected);
        clientMessage.appendLong(updated);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    static decodeResponse(clientMessage: ClientInputMessage, toObjectFunction: (data: Data) => any = null) {
        // Decode response from client message
        var parameters: any = {
            'response': null
        };

        parameters['response'] = clientMessage.readBoolean();

        return parameters;
    }


}
