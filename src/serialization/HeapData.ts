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
import {Data} from './Data';

export const PARTITION_HASH_OFFSET: number = 0;
export const TYPE_OFFSET: number = 4;
export const DATA_OFFSET: number = 8;
export const HEAP_DATA_OVERHEAD: number = DATA_OFFSET;

export class HeapData implements Data {

    private payload: Buffer;

    constructor(buffer: Buffer) {
        if (buffer != null && buffer.length > 0 && buffer.length < HEAP_DATA_OVERHEAD) {
            throw new RangeError('Data should be either empty or should contain more than ' + HEAP_DATA_OVERHEAD
                + ' bytes! -> '
                + buffer);
        }
        this.payload = buffer;
    }

    /**
     * Returns serialized representation in a buffer
     */
    public toBuffer(): Buffer {
        return this.payload;
    }

    public writeToBuffer(buffer: Buffer, offset: number): void {
        throw new Error('Not supported');
    }

    public isLazy(): boolean {
        return false;
    }

    /**
     * Returns serialization type
     */
    public getType(): number {
        if (this.totalSize() === 0) {
            // TODO serialization null type
            return 0;
        }
        return this.payload.readIntBE(TYPE_OFFSET, 4);
    }

    /**
     * Returns the total size of data in bytes
     */
    public totalSize(): number {
        if (this.payload === null) {
            return 0;
        } else {
            return this.payload.length;
        }
    }

    /**
     * Returns size of internal binary data in bytes
     */
    public dataSize(): number {
        return Math.max(this.totalSize() - HEAP_DATA_OVERHEAD, 0);
    }

    /**
     * Returns approximate heap cost of this Data object in bytes
     */
    getHeapCost(): number {
        return 0;
    }

    /**
     * Returns partition hash of serialized object
     */
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

    /**
     * Returns true if data has partition hash
     */
    hasPartitionHash(): boolean {
        return this.payload !== null
            && this.payload.length >= HEAP_DATA_OVERHEAD
            && this.payload.readIntBE(PARTITION_HASH_OFFSET, 4) !== 0;
    }

    /**
     * Returns true if the object is a portable object
     */
    isPortable(): boolean {
        return false;
    }

}

import {BitsUtil} from '../BitsUtil';

export const LAZY_STRING_SERIALIZER_ID: number = -11;
export const LAZY_HEADER_SIZE: number = DATA_OFFSET + 4;

export class LazyStringHeapData implements Data {

    private readonly partitionHash: number;
    private readonly data: string;
    private readonly dataBinSize: number;

    constructor(data: any, partitionHash: number = 0) {
        this.data = data as string;
        this.partitionHash = partitionHash;
        this.dataBinSize = Buffer.byteLength(this.data, 'utf8');
    }

    public toBuffer(): Buffer {
        throw new Error('Not supported');
    }

    public writeToBuffer(buffer: Buffer, offset: number): void {
        BitsUtil.writeInt32(buffer, offset, this.partitionHash, true);
        BitsUtil.writeInt32(buffer, offset + TYPE_OFFSET, LAZY_STRING_SERIALIZER_ID, true);
        BitsUtil.writeInt32(buffer, offset + DATA_OFFSET, this.data.length, true);
        buffer.write(this.data, offset + LAZY_HEADER_SIZE, this.dataBinSize, 'utf8');
    }

    public isLazy(): boolean {
        return true;
    }

    /**
     * Returns serialization type
     */
    public getType(): number {
        return LAZY_STRING_SERIALIZER_ID;
    }

    /**
     * Returns the total size of data in bytes
     */
    public totalSize(): number {
        return this.dataBinSize + LAZY_HEADER_SIZE;
    }

    /**
     * Returns size of internal binary data in bytes
     */
    public dataSize(): number {
        return this.dataBinSize + (LAZY_HEADER_SIZE - HEAP_DATA_OVERHEAD);
    }

    /**
     * Returns approximate heap cost of this Data object in bytes
     */
    getHeapCost(): number {
        return 0;
    }

    getPartitionHash(): number {
        if (this.hasPartitionHash()) {
            return this.partitionHash;
        } else {
            return this.hashCode();
        }
    }

    hashCode(): number {
        return 0;
    }

    equals(other: Data): boolean {
        return false;
    }

    hasPartitionHash(): boolean {
        return this.partitionHash !== 0;
    }

    /**
     * Returns true if the object is a portable object
     */
    isPortable(): boolean {
        return false;
    }

}
