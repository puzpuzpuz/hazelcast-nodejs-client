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

/* tslint:disable:no-bitwise */
/*
 Client Message is the carrier framed data as defined below.
 Any request parameter, response or event data will be carried in the payload.
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |R|                      Frame Length                           |
 +-------------+---------------+---------------------------------+
 |  Version    |B|E|  Flags    |               Type              |
 +-------------+---------------+---------------------------------+
 |                       CorrelationId                           |
 |                                                               |
 +---------------------------------------------------------------+
 |                        PartitionId                            |
 +-----------------------------+---------------------------------+
 |        Data Offset          |                                 |
 +-----------------------------+                                 |
 |                      Message Payload Data                    ...
 |                                                              ...
 */

import {Buffer} from 'safe-buffer';
import * as Long from 'long';
import {BitsUtil} from './BitsUtil';
import {Data} from './serialization/Data';
import {HeapData} from './serialization/HeapData';

export class ClientInputMessage {

    private buffer: Buffer;
    private cursor: number = BitsUtil.HEADER_SIZE;
    private isRetryable: boolean;

    constructor(buffer: Buffer) {
        this.buffer = buffer;
    }

    getBuffer(): Buffer {
        return this.buffer;
    }

    getCorrelationId(): Long {
        const offset = BitsUtil.CORRELATION_ID_FIELD_OFFSET;
        return this.readLongInternal(offset);
    }

    setCorrelationId(value: Long): void {
        this.writeLongInternal(value, BitsUtil.CORRELATION_ID_FIELD_OFFSET);
    }

    getPartitionId(): number {
        return this.buffer.readInt32LE(BitsUtil.PARTITION_ID_FIELD_OFFSET);
    }

    setPartitionId(value: number): void {
        this.buffer.writeInt32LE(value, BitsUtil.PARTITION_ID_FIELD_OFFSET);
    }

    setVersion(value: number): void {
        this.buffer.writeUInt8(value, BitsUtil.VERSION_FIELD_OFFSET);
    }

    getMessageType(): number {
        return this.buffer.readUInt16LE(BitsUtil.TYPE_FIELD_OFFSET);
    }

    setMessageType(value: number): void {
        this.buffer.writeUInt16LE(value, BitsUtil.TYPE_FIELD_OFFSET);
    }

    getFlags(): number {
        return this.buffer.readUInt8(BitsUtil.FLAGS_FIELD_OFFSET);
    }

    setFlags(value: number): void {
        this.buffer.writeUInt8(value, BitsUtil.FLAGS_FIELD_OFFSET);
    }

    hasFlags(flags: number): number {
        return this.getFlags() & flags;
    }

    getFrameLength(): number {
        return this.buffer.readInt32LE(BitsUtil.FRAME_LENGTH_FIELD_OFFSET);
    }

    setFrameLength(value: number): void {
        this.buffer.writeInt32LE(value, BitsUtil.FRAME_LENGTH_FIELD_OFFSET);
    }

    getDataOffset(): number {
        return this.buffer.readInt16LE(BitsUtil.DATA_OFFSET_FIELD_OFFSET);
    }

    setDataOffset(value: number): void {
        this.buffer.writeInt16LE(value, BitsUtil.DATA_OFFSET_FIELD_OFFSET);
    }

    setRetryable(value: boolean): void {
        this.isRetryable = value;
    }

    appendByte(value: number): void {
        this.buffer.writeUInt8(value, this.cursor);
        this.cursor += BitsUtil.BYTE_SIZE_IN_BYTES;
    }

    appendBoolean(value: boolean): void {
        return this.appendByte(value ? 1 : 0);
    }

    appendInt32(value: number): void {
        this.buffer.writeInt32LE(value, this.cursor);
        this.cursor += BitsUtil.INT_SIZE_IN_BYTES;
    }

    appendUint8(value: number): void {
        this.buffer.writeUInt8(value, this.cursor);
        this.cursor += BitsUtil.BYTE_SIZE_IN_BYTES;
    }

    appendLong(value: any): void {
        this.writeLongInternal(value, this.cursor);
        this.cursor += BitsUtil.LONG_SIZE_IN_BYTES;
    }

    appendString(value: string): void {
        const length = value.length;
        this.buffer.writeInt32LE(length, this.cursor);
        this.cursor += 4;
        this.buffer.write(value, this.cursor);
        this.cursor += length;
    }

    appendBuffer(buffer: Buffer): void {
        const length = buffer.length;
        this.appendInt32(length);
        buffer.copy(this.buffer, this.cursor);
        this.cursor += length;
    }

    appendData(data: Data): void {
        this.appendBuffer(data.toBuffer());
    }

    addFlag(value: number): void {
        this.buffer.writeUInt8(value | this.getFlags(), BitsUtil.FLAGS_FIELD_OFFSET);
    }

    updateFrameLength(): void {
        this.setFrameLength(this.cursor);
    }

    readData(): Data {
        const dataPayload: Buffer = this.readBuffer();
        return new HeapData(dataPayload);
    }

    readByte(): number {
        const value = this.buffer.readUInt8(this.cursor);
        this.cursor += BitsUtil.BYTE_SIZE_IN_BYTES;
        return value;
    }

    readBoolean(): boolean {
        return this.readByte() === 1;
    }

    readUInt8(): number {
        const value = this.buffer.readUInt8(this.cursor);
        this.cursor += BitsUtil.BYTE_SIZE_IN_BYTES;
        return value;
    }

    readInt32(): number {
        const value = this.buffer.readInt32LE(this.cursor);
        this.cursor += BitsUtil.INT_SIZE_IN_BYTES;
        return value;
    }

    readLong(): Long {
        const value = this.readLongInternal(this.cursor);
        this.cursor += BitsUtil.LONG_SIZE_IN_BYTES;
        return value;
    }

    readString(): string {
        const length = this.buffer.readInt32LE(this.cursor);
        this.cursor += BitsUtil.INT_SIZE_IN_BYTES;
        const value = this.buffer.toString('utf8', this.cursor, this.cursor + length);
        this.cursor += length;
        return value;
    }

    readBuffer(): Buffer {
        const size = this.buffer.readUInt32LE(this.cursor);
        this.cursor += BitsUtil.INT_SIZE_IN_BYTES;
        const result = Buffer.allocUnsafe(size);
        this.buffer.copy(result, 0, this.cursor, this.cursor + size);
        this.cursor += size;
        return result;
    }

    isComplete(): boolean {
        return (this.cursor >= BitsUtil.HEADER_SIZE) && (this.cursor === this.getFrameLength());
    }

    readMapEntry(): any {
        // TODO
    }

    private writeLongInternal(value: any, offset: number): void {
        if (!Long.isLong(value)) {
            value = Long.fromValue(value);
        }

        this.buffer.writeInt32LE(value.low, offset);
        this.buffer.writeInt32LE(value.high, offset + 4);
    }

    private readLongInternal(offset: number): Long {
        const low = this.buffer.readInt32LE(offset);
        const high = this.buffer.readInt32LE(offset + 4);
        return new Long(low, high);
    }
}

export class ClientOutputMessage {

    private header: Buffer;
    private payload: Buffer[] = [];
    private totalSize: number;
    private correlationId: Long;
    private isRetryable: boolean;

    public static initialMessage(): ClientOutputMessage {
        const message = new ClientOutputMessage();
        message.header = Buffer.from('CB2');
        message.totalSize = message.header.length;
        return message;
    }

    public static newClientMessage(payloadSize: number): ClientOutputMessage {
        const message = new ClientOutputMessage();
        message.totalSize = BitsUtil.HEADER_SIZE + payloadSize;
        message.header = Buffer.allocUnsafe(BitsUtil.HEADER_SIZE);
        message.setDataOffset(BitsUtil.HEADER_SIZE);
        message.setVersion(BitsUtil.VERSION);
        message.setFrameLength(message.totalSize);
        message.setFlags(0xc0);
        message.setPartitionId(-1);
        return message;
    }

    // TODO this implementation is not safe
    copy(): ClientOutputMessage {
        const message = new ClientOutputMessage();
        message.header = Buffer.from(this.header);
        message.payload = this.payload;
        message.isRetryable = this.isRetryable;
        return message;
    }

    getBuffers(): Buffer[] {
        return [this.header, ...this.payload];
    }

    getTotalSize(): number {
        return this.totalSize;
    }

    getCorrelationId(): Long {
        return this.correlationId;
    }

    setCorrelationId(value: Long): void {
        if (!Long.isLong(value)) {
            value = Long.fromValue(value);
        }

        this.header.writeInt32LE(value.low, BitsUtil.CORRELATION_ID_FIELD_OFFSET);
        this.header.writeInt32LE(value.high, BitsUtil.CORRELATION_ID_FIELD_OFFSET + 4);
        this.correlationId = value;
    }

    setPartitionId(value: number): void {
        this.header.writeInt32LE(value, BitsUtil.PARTITION_ID_FIELD_OFFSET);
    }

    setVersion(value: number): void {
        this.header.writeUInt8(value, BitsUtil.VERSION_FIELD_OFFSET);
    }

    setMessageType(value: number): void {
        this.header.writeUInt16LE(value, BitsUtil.TYPE_FIELD_OFFSET);
    }

    setFlags(value: number): void {
        this.header.writeUInt8(value, BitsUtil.FLAGS_FIELD_OFFSET);
    }

    setFrameLength(value: number): void {
        this.header.writeInt32LE(value, BitsUtil.FRAME_LENGTH_FIELD_OFFSET);
    }

    setDataOffset(value: number): void {
        this.header.writeInt16LE(value, BitsUtil.DATA_OFFSET_FIELD_OFFSET);
    }

    setRetryable(value: boolean): void {
        this.isRetryable = value;
    }

    appendByte(value: number): void {
        const buf = Buffer.allocUnsafe(BitsUtil.BYTE_SIZE_IN_BYTES);
        buf.writeUInt8(value, 0);
        this.payload.push(buf);
    }

    appendBoolean(value: boolean): void {
        return this.appendByte(value ? 1 : 0);
    }

    appendInt32(value: number): void {
        const buf = Buffer.allocUnsafe(BitsUtil.INT_SIZE_IN_BYTES);
        buf.writeInt32LE(value, 0);
        this.payload.push(buf);
    }

    appendUint8(value: number): void {
        const buf = Buffer.allocUnsafe(BitsUtil.BYTE_SIZE_IN_BYTES);
        buf.writeUInt8(value, 0);
        this.payload.push(buf);
    }

    appendLong(value: any): void {
        const buf = Buffer.allocUnsafe(BitsUtil.LONG_SIZE_IN_BYTES);
        
        if (!Long.isLong(value)) {
            value = Long.fromValue(value);
        }

        buf.writeInt32LE(value.low, 0);
        buf.writeInt32LE(value.high, 4);
        this.payload.push(buf);
    }

    appendString(value: string): void {
        const length = value.length;
        const buf = Buffer.allocUnsafe(Buffer.byteLength(value, 'utf8') + 4);
        buf.writeInt32LE(length, 0);
        buf.write(value, 4);
        this.payload.push(buf);
    }

    appendBuffer(buffer: Buffer): void {
        this.appendInt32(buffer.length);
        this.payload.push(buffer);
    }

    appendData(data: Data): void {
        this.appendBuffer(data.toBuffer());
    }

    updateFrameLength(): void {
        // TODO: no-op for now
    }
}
