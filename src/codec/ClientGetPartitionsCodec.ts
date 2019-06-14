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
import Address = require('../Address');
import {AddressCodec} from './AddressCodec';
import {Data} from '../serialization/Data';
import {ClientMessageType} from './ClientMessageType';

var REQUEST_TYPE = ClientMessageType.CLIENT_GETPARTITIONS;
var RESPONSE_TYPE = 108;
var RETRYABLE = false;


export class ClientGetPartitionsCodec {


    static calculateSize() {
// Calculates the request payload size
        var dataSize: number = 0;
        return dataSize;
    }

    static encodeRequest() {
// Encode request into clientMessage
        var clientMessage = ClientOutputMessage.newClientMessage(this.calculateSize());
        clientMessage.setMessageType(REQUEST_TYPE);
        clientMessage.setRetryable(RETRYABLE);
        clientMessage.updateFrameLength();
        return clientMessage;
    }

    static decodeResponse(clientMessage: ClientInputMessage, toObjectFunction: (data: Data) => any = null) {
        // Decode response from client message
        var parameters: any = {
            'partitions': null,
            'partitionStateVersion': null
        };


        var partitionsSize = clientMessage.readInt32();
        var partitions: any = [];
        for (var partitionsIndex = 0; partitionsIndex < partitionsSize; partitionsIndex++) {
            var partitionsItem: any;
            var partitionsItemKey: Address;
            var partitionsItemVal: any;
            partitionsItemKey = AddressCodec.decode(clientMessage, toObjectFunction);

            var partitionsItemValSize = clientMessage.readInt32();
            var partitionsItemVal: any = [];
            for (var partitionsItemValIndex = 0; partitionsItemValIndex < partitionsItemValSize; partitionsItemValIndex++) {
                var partitionsItemValItem: number;
                partitionsItemValItem = clientMessage.readInt32();
                partitionsItemVal.push(partitionsItemValItem);
            }
            partitionsItem = [partitionsItemKey, partitionsItemVal];
            partitions.push(partitionsItem);
        }
        parameters['partitions'] = partitions;

        if (clientMessage.isComplete()) {
            return parameters;
        }
        parameters['partitionStateVersion'] = clientMessage.readInt32();
        parameters.partitionStateVersionExist = true;
        return parameters;
    }


}
