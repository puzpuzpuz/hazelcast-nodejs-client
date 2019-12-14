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

import {Buffer} from 'safe-buffer';
import murmur = require('../invocation/Murmur');
import {Data, DataOutput} from './Data';

export const PARTITION_HASH_OFFSET: number = 0;
export const TYPE_OFFSET: number = 4;
export const DATA_OFFSET: number = 8;
export const HEAP_DATA_OVERHEAD: number = DATA_OFFSET;

export class HeapData implements Data {

    private payload: Buffer;

    constructor(buffer: Buffer) {
        if (buffer != null && buffer.length > 0 && buffer.length < HEAP_DATA_OVERHEAD) {
            throw new RangeError('Data should be either empty or should contain more than '
                + HEAP_DATA_OVERHEAD + ' bytes! -> ' + buffer.length);
        }
        this.payload = buffer;
    }

    toBuffer(): Buffer {
        return this.payload;
    }

    writeTo(buffer: Buffer): number {
        // TODO assert wrote all bytes
        return this.payload.copy(buffer);
    }

    getType(): number {
        if (this.totalSize() === 0) {
            // TODO serialization null type
            return 0;
        }
        return this.payload.readIntBE(TYPE_OFFSET, 4);
    }

    totalSize(): number {
        if (this.payload === null) {
            return 0;
        } else {
            return this.payload.length;
        }
    }

    dataSize(): number {
        return Math.max(this.totalSize() - HEAP_DATA_OVERHEAD, 0);
    }

    getHeapCost(): number {
        return 0;
    }

    getPartitionHash(): number {
        if (this.hasPartitionHash()) {
            return this.payload.readIntBE(PARTITION_HASH_OFFSET, 4);
        } else {
            return this.hashCode();
        }
    }

    hashCode(): number {
        return murmur(this.payload.slice(DATA_OFFSET));
    }

    equals(other: Data): boolean {
        return this.payload.compare(other.toBuffer(), DATA_OFFSET, other.toBuffer().length, DATA_OFFSET) === 0;
    }

    hasPartitionHash(): boolean {
        return this.payload !== null
            && this.payload.length >= HEAP_DATA_OVERHEAD
            && this.payload.readIntBE(PARTITION_HASH_OFFSET, 4) !== 0;
    }

    isPortable(): boolean {
        return false;
    }

}

export class LazyHeapData implements Data {

    private chunkedPayload: DataOutput;
    private payload: Buffer; // lazily calculated if dataOutput is set

    constructor(dataOutput: DataOutput) {
        if (dataOutput.size() < HEAP_DATA_OVERHEAD) {
            throw new RangeError('Data should be either empty or should contain more than '
                + HEAP_DATA_OVERHEAD + ' bytes! -> ' + dataOutput.size());
        }
        this.chunkedPayload = dataOutput;
        this.payload = null;
    }

    toBuffer(): Buffer {
        this.calculatePayload();
        return this.payload;
    }

    public writeTo(buffer: Buffer): number {
        if (this.payload !== null) {
            return this.payload.copy(buffer);
        }
        return this.chunkedPayload.writeTo(buffer);
    }

    getType(): number {
        if (this.totalSize() === 0) {
            // TODO serialization null type
            return 0;
        }
        this.calculatePayload();
        return this.payload.readIntBE(TYPE_OFFSET, 4);
    }

    totalSize(): number {
        return this.payload !== null ? this.payload.length : this.chunkedPayload.size();
    }

    dataSize(): number {
        return Math.max(this.totalSize() - HEAP_DATA_OVERHEAD, 0);
    }

    getHeapCost(): number {
        return 0;
    }

    getPartitionHash(): number {
        this.calculatePayload();
        if (this.hasPartitionHash()) {
            return this.payload.readIntBE(PARTITION_HASH_OFFSET, 4);
        } else {
            return this.hashCode();
        }
    }

    hashCode(): number {
        this.calculatePayload();
        return murmur(this.payload.slice(DATA_OFFSET));
    }

    equals(other: Data): boolean {
        this.calculatePayload();
        return this.payload.compare(other.toBuffer(), DATA_OFFSET, other.toBuffer().length, DATA_OFFSET) === 0;
    }

    hasPartitionHash(): boolean {
        this.calculatePayload();
        return this.payload.length >= HEAP_DATA_OVERHEAD
            && this.payload.readIntBE(PARTITION_HASH_OFFSET, 4) !== 0;
    }

    isPortable(): boolean {
        return false;
    }

    private calculatePayload() {
        if (this.payload === null) {
            this.payload = this.chunkedPayload.toBuffer();
            this.chunkedPayload = null;
        }
        return this.payload;
    }

}
