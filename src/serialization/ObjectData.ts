/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import * as assert from 'assert';
import * as Long from 'long';
import {BitsUtil} from '../BitsUtil';
import {Data, DataInput, DataOutput, PositionalDataOutput} from './Data';
import {HeapData} from './HeapData';
import {SerializationService} from './SerializationService';

const MASK_1BYTE = (1 << 8) - 1;
const MASK_2BYTE = (1 << 16) - 1;
const MASK_4BYTE = (1 << 32) - 1;

export class ObjectDataOutput implements DataOutput {
    // protected buffer: Buffer;
    protected bigEndian: boolean;
    private service: SerializationService;
    private pos: number;

    protected static buffer = Buffer.allocUnsafe(2048);

    constructor(length: number, service: SerializationService, isBigEndian: boolean) {
        // this.buffer = new Buffer(length);
        this.service = service;
        this.bigEndian = isBigEndian;
        this.pos = 0;
    }

    clear(): void {
        // this.buffer = new Buffer(this.buffer.length);
        this.pos = 0;
    }

    isBigEndian(): boolean {
        return this.bigEndian;
    }

    position(newPosition?: number): number {
        const oldPos = this.pos;
        if (Number.isInteger(newPosition)) {
            this.pos = newPosition;
        }
        return oldPos;
    }

    toBuffer(): Buffer {
        if (this.pos === 0) {
            return ObjectDataOutput.buffer.slice(0, 0);
        } else {
            return ObjectDataOutput.buffer.slice(0, this.pos);
        }
    }

    write(byte: number | Buffer): void {
        if (Buffer.isBuffer(byte)) {
            this.ensureAvailable(byte.length);
            byte.copy(ObjectDataOutput.buffer, this.pos);
            this.pos += byte.length;
        } else {
            this.ensureAvailable(BitsUtil.BYTE_SIZE_IN_BYTES);
            BitsUtil.writeUInt8(ObjectDataOutput.buffer, this.pos, byte & MASK_1BYTE);
            this.pos += BitsUtil.BYTE_SIZE_IN_BYTES;
        }
    }

    writeBoolean(val: boolean): void {
        this.write(val ? 1 : 0);
    }

    writeBooleanArray(val: boolean[]): void {
        this.writeArray(this.writeBoolean, val);
    }

    writeByte(byte: number): void {
        this.write(byte);
    }

    writeByteArray(bytes: number[]): void {
        this.writeArray(this.writeByte, bytes);
    }

    writeBytes(bytes: string): void {
        const len = (bytes != null) ? bytes.length : 0;
        for (let i = 0; i < len; i++) {
            this.write(bytes.charCodeAt(i));
        }
    }

    writeChar(char: string): void {
        this.ensureAvailable(BitsUtil.CHAR_SIZE_IN_BYTES);
        BitsUtil.writeUInt16(ObjectDataOutput.buffer, this.pos, char.charCodeAt(0), this.isBigEndian());
        this.pos += BitsUtil.CHAR_SIZE_IN_BYTES;
    }

    writeCharArray(chars: string[]): void {
        this.writeArray(this.writeChar, chars);
    }

    writeChars(chars: string): void {
        const len = (chars != null) ? chars.length : BitsUtil.NULL_ARRAY_LENGTH;
        this.writeInt(len);
        for (let i = 0; i < len; i++) {
            this.writeChar(chars.charAt(i));
        }
    }

    writeData(data: Data): void {
        const buf = (data != null) ? data.toBuffer() : null;
        const len = (buf != null) ? buf.length : BitsUtil.NULL_ARRAY_LENGTH;
        this.writeInt(len);
        for (let i = 0; i < len; i++) {
            this.write(buf[i]);
        }
    }

    writeDouble(double: number): void {
        this.ensureAvailable(BitsUtil.DOUBLE_SIZE_IN_BYTES);
        BitsUtil.writeDouble(ObjectDataOutput.buffer, this.pos, double, this.isBigEndian());
        this.pos += BitsUtil.DOUBLE_SIZE_IN_BYTES;
    }

    writeDoubleArray(doubles: number[]): void {
        this.writeArray(this.writeDouble, doubles);
    }

    writeFloat(float: number): void {
        this.ensureAvailable(BitsUtil.FLOAT_SIZE_IN_BYTES);
        BitsUtil.writeFloat(ObjectDataOutput.buffer, this.pos, float, this.isBigEndian());
        this.pos += BitsUtil.FLOAT_SIZE_IN_BYTES;
    }

    writeFloatArray(floats: number[]): void {
        this.writeArray(this.writeFloat, floats);
    }

    writeInt(int: number): void {
        this.ensureAvailable(BitsUtil.INT_SIZE_IN_BYTES);
        BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, int, this.isBigEndian());
        this.pos += BitsUtil.INT_SIZE_IN_BYTES;
    }

    writeIntBE(int: number): void {
        this.ensureAvailable(BitsUtil.INT_SIZE_IN_BYTES);
        BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, int, true);
        this.pos += BitsUtil.INT_SIZE_IN_BYTES;
    }

    writeIntArray(ints: number[]): void {
        this.writeArray(this.writeInt, ints);
    }

    writeLong(long: Long): void {
        this.ensureAvailable(BitsUtil.LONG_SIZE_IN_BYTES);
        if (this.isBigEndian()) {
            BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, long.high, true);
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
            BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, long.low, true);
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
        } else {
            BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, long.low, false);
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
            BitsUtil.writeInt32(ObjectDataOutput.buffer, this.pos, long.high, false);
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
        }
    }

    writeLongArray(longs: Long[]): void {
        this.writeArray(this.writeLong, longs);
    }

    writeObject(object: any): void {
        this.service.writeObject(this, object);
    }

    writeShort(short: number): void {
        this.ensureAvailable(BitsUtil.SHORT_SIZE_IN_BYTES);
        BitsUtil.writeInt16(ObjectDataOutput.buffer, this.pos, short, this.isBigEndian());
        this.pos += BitsUtil.SHORT_SIZE_IN_BYTES;
    }

    writeShortArray(shorts: number[]): void {
        this.writeArray(this.writeShort, shorts);
    }

    writeUTF(val: string): void {
        const len = (val != null) ? val.length : BitsUtil.NULL_ARRAY_LENGTH;
        this.writeInt(len);
        this.ensureAvailable(len * 3);
        for (let i = 0; i < len; i++) {
            const ch = val.charCodeAt(i);
            if (ch <= 0x007F) {
                this.writeByte(ch);
            } else if (ch <= 0x07FF) {
                this.write(0xC0 | ch >> 6 & 0x1F);
                this.write(0x80 | ch & 0x3F);
            } else {
                this.write(0xE0 | ch >> 12 & 0x0F);
                this.write(0x80 | ch >> 6 & 0x3F);
                this.write(0x80 | ch & 0x3F);
            }
        }
    }

    writeUTFArray(val: string[]): void {
        this.writeArray(this.writeUTF, val);
    }

    writeZeroBytes(count: number): void {
        for (let i = 0; i < count; i++) {
            this.write(0);
        }
    }

    private available(): number {
        return ObjectDataOutput.buffer == null ? 0 : ObjectDataOutput.buffer.length - this.pos;
    }

    private ensureAvailable(size: number): void {
        if (this.available() < size) {
            const newBuffer = Buffer.allocUnsafe(this.pos + size);
            ObjectDataOutput.buffer.copy(newBuffer, 0, 0, this.pos);
            ObjectDataOutput.buffer = newBuffer;
        }
    }

    private writeArray(func: Function, arr: any[]): void {
        const len = (arr != null) ? arr.length : BitsUtil.NULL_ARRAY_LENGTH;
        this.writeInt(len);
        if (len > 0) {
            const boundFunc = func.bind(this);
            arr.forEach(boundFunc);
        }
    }
}

export class PositionalObjectDataOutput extends ObjectDataOutput implements PositionalDataOutput {
    pwrite(position: number, byte: number | Buffer): void {
        if (Buffer.isBuffer(byte)) {
            byte.copy(ObjectDataOutput.buffer, position);
        } else {
            ObjectDataOutput.buffer[position] = byte;
        }
    }

    pwriteBoolean(position: number, val: boolean): void {
        this.pwrite(position, val ? 1 : 0);
    }

    pwriteByte(position: number, byte: number): void {
        this.pwrite(position, byte);
    }

    pwriteChar(position: number, char: string): void {
        BitsUtil.writeUInt16(ObjectDataOutput.buffer, position, char.charCodeAt(0), this.isBigEndian());
    }

    pwriteDouble(position: number, double: number): void {
        BitsUtil.writeDouble(ObjectDataOutput.buffer, position, double, this.isBigEndian());
    }

    pwriteFloat(position: number, float: number): void {
        BitsUtil.writeFloat(ObjectDataOutput.buffer, position, float, this.isBigEndian());
    }

    pwriteInt(position: number, int: number): void {
        BitsUtil.writeInt32(ObjectDataOutput.buffer, position, int, this.isBigEndian());
    }

    pwriteIntBE(position: number, int: number): void {
        BitsUtil.writeInt32(ObjectDataOutput.buffer, position, int, true);
    }

    pwriteLong(position: number, long: Long): void {
        if (this.isBigEndian()) {
            BitsUtil.writeInt32(ObjectDataOutput.buffer, position, long.high, true);
            BitsUtil.writeInt32(ObjectDataOutput.buffer, position + BitsUtil.INT_SIZE_IN_BYTES, long.low, true);
        } else {
            BitsUtil.writeInt32(ObjectDataOutput.buffer, position, long.low, false);
            BitsUtil.writeInt32(ObjectDataOutput.buffer, position + BitsUtil.INT_SIZE_IN_BYTES, long.high, false);
        }
    }

    pwriteShort(position: number, short: number): void {
        BitsUtil.writeInt16(ObjectDataOutput.buffer, position, short, this.isBigEndian());
    }
}

export class ObjectDataInput implements DataInput {

    // private buffer: Buffer;
    private offset: number;
    private service: SerializationService;
    private bigEndian: boolean;
    private pos: number;

    protected static buffer = Buffer.allocUnsafe(2048);

    constructor(buffer: Buffer, offset: number, serializationService: SerializationService, isBigEndian: boolean) {
        this.offset = offset;
        this.service = serializationService;
        this.bigEndian = isBigEndian;
        this.pos = this.offset;
    }

    isBigEndian(): boolean {
        return this.bigEndian;
    }

    position(newPosition?: number): number {
        const oldPos = this.pos;
        if (Number.isInteger(newPosition)) {
            this.pos = newPosition;
        }
        return oldPos;
    }

    read(pos?: number): number {
        this.assertAvailable(BitsUtil.BYTE_SIZE_IN_BYTES, pos);
        if (pos === undefined) {
            return BitsUtil.readUInt8(ObjectDataInput.buffer, this.pos++);
        } else {
            return BitsUtil.readUInt8(ObjectDataInput.buffer, pos);
        }
    }

    readBoolean(pos?: number): boolean {
        return this.read(pos) === 1;
    }

    readBooleanArray(pos?: number): boolean[] {
        return this.readArray<boolean>(this.readBoolean, pos);
    }

    readByte(pos?: number): number {
        return this.read(pos);
    }

    readByteArray(pos?: number): number[] {
        return this.readArray<number>(this.readByte, pos);
    }

    readChar(pos?: number): string {
        this.assertAvailable(BitsUtil.CHAR_SIZE_IN_BYTES);
        let readBytes: any;
        if (pos === undefined) {
            readBytes = BitsUtil.readUInt16(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.CHAR_SIZE_IN_BYTES;
        } else {
            readBytes = BitsUtil.readUInt16(ObjectDataInput.buffer, pos, this.isBigEndian());
        }
        return String.fromCharCode(readBytes);
    }

    readCharArray(pos?: number): string[] {
        return this.readArray<string>(this.readChar, pos);
    }

    readData(): Data {
        const bytes: number[] = this.readByteArray();
        const data: Data = bytes === null ? null : new HeapData(new Buffer(bytes));
        return data;
    }

    readDouble(pos?: number): number {
        this.assertAvailable(BitsUtil.DOUBLE_SIZE_IN_BYTES, pos);
        let ret: number;
        if (pos === undefined) {
            ret = BitsUtil.readDouble(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.DOUBLE_SIZE_IN_BYTES;
        } else {
            ret = BitsUtil.readDouble(ObjectDataInput.buffer, pos, this.isBigEndian());
        }
        return ret;
    }

    readDoubleArray(pos?: number): number[] {
        return this.readArray<number>(this.readDouble, pos);
    }

    readFloat(pos?: number): number {
        this.assertAvailable(BitsUtil.FLOAT_SIZE_IN_BYTES, pos);
        let ret: number;
        if (pos === undefined) {
            ret = BitsUtil.readFloat(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.FLOAT_SIZE_IN_BYTES;
        } else {
            ret = BitsUtil.readFloat(ObjectDataInput.buffer, pos, this.isBigEndian());
        }
        return ret;
    }

    readFloatArray(pos?: number): number[] {
        return this.readArray<number>(this.readFloat, pos);
    }

    readInt(pos?: number): number {
        this.assertAvailable(BitsUtil.INT_SIZE_IN_BYTES, pos);
        let ret: number;
        if (pos === undefined) {
            ret = BitsUtil.readInt32(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
        } else {
            ret = BitsUtil.readInt32(ObjectDataInput.buffer, pos, this.isBigEndian());
        }
        return ret;
    }

    readIntArray(pos?: number): number[] {
        return this.readArray<number>(this.readInt, pos);
    }

    readLong(pos?: number): Long {
        this.assertAvailable(BitsUtil.LONG_SIZE_IN_BYTES, pos);
        let first: number;
        let second: number;
        if (pos === undefined) {
            first = BitsUtil.readInt32(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
            second = BitsUtil.readInt32(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.INT_SIZE_IN_BYTES;
        } else {
            first = BitsUtil.readInt32(ObjectDataInput.buffer, pos, this.isBigEndian());
            second = BitsUtil.readInt32(ObjectDataInput.buffer, pos + BitsUtil.INT_SIZE_IN_BYTES, this.isBigEndian());
        }
        if (this.isBigEndian()) {
            return new Long(second, first);
        } else {
            return new Long(first, second);
        }
    }

    readLongArray(pos?: number): Long[] {
        return this.readArray<Long>(this.readLong, pos);
    }

    readObject(): any {
        return this.service.readObject(this);
    }

    readShort(pos?: number): number {
        this.assertAvailable(BitsUtil.SHORT_SIZE_IN_BYTES, pos);
        let ret: number;
        if (pos === undefined) {
            ret = BitsUtil.readInt16(ObjectDataInput.buffer, this.pos, this.isBigEndian());
            this.pos += BitsUtil.SHORT_SIZE_IN_BYTES;
        } else {
            ret = BitsUtil.readInt16(ObjectDataInput.buffer, pos, this.isBigEndian());
        }
        return ret;
    }

    readShortArray(pos?: number): number[] {
        return this.readArray<number>(this.readShort, pos);
    }

    readUnsignedByte(pos?: number): number {
        return this.read(pos);
    }

    readUnsignedShort(pos?: number): number {
        return this.readChar(pos).charCodeAt(0);
    }

    readUTF(pos?: number): string {
        const len = this.readInt(pos);
        let readingIndex = this.addOrUndefined(pos, 4);
        if (len === BitsUtil.NULL_ARRAY_LENGTH) {
            return null;
        }
        let result: string = '';
        let leadingByte: number;
        for (let i = 0; i < len; i++) {
            let charCode: number;
            leadingByte = this.readByte(readingIndex) & MASK_1BYTE;
            readingIndex = this.addOrUndefined(readingIndex, 1);

            const b = leadingByte & 0xFF;
            switch (b >> 4) {
                /* tslint:disable:no-switch-case-fall-through */
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    charCode = leadingByte;
                    break;
                case 12:
                case 13:
                    const first = (b & 0x1F) << 6;
                    const second = this.readByte(readingIndex) & 0x3F;
                    readingIndex = this.addOrUndefined(readingIndex, 1);
                    charCode = first | second;
                    break;
                case 14:
                    const first2 = (b & 0x0F) << 12;
                    const second2 = (this.readByte(readingIndex) & 0x3F) << 6;
                    readingIndex = this.addOrUndefined(readingIndex, 1);
                    const third2 = this.readByte(readingIndex) & 0x3F;
                    readingIndex = this.addOrUndefined(readingIndex, 1);
                    charCode = (first2 | second2 | third2);
                    break;
                default:
                    throw new RangeError('Malformed UTF8 string');
            }
            result += String.fromCharCode(charCode);
        }
        return result;
    }

    readUTFArray(pos?: number): string[] {
        return this.readArray<string>(this.readUTF, pos);
    }

    reset(): void {
        this.pos = 0;
    }

    skipBytes(count: number): void {
        this.pos += count;
    }

    readCopy(other: Buffer, numBytes: number): void {
        this.assertAvailable(numBytes, this.pos);
        ObjectDataInput.buffer.copy(other, 0, this.pos, this.pos + numBytes);
        this.pos += numBytes;
    }

    available(): number {
        return ObjectDataInput.buffer.length - this.pos;
    }

    private readArray<T>(func: Function, pos?: number): T[] {
        const backupPos = this.pos;
        if (pos !== undefined) {
            this.pos = pos;
        }
        const len = this.readInt();
        const arr: T[] = [];
        for (let i = 0; i < len; i++) {
            arr.push(func.call(this));
        }
        if (pos !== undefined) {
            this.pos = backupPos;
        }
        return arr;
    }

    private assertAvailable(numOfBytes: number, pos: number = this.pos): void {
        assert(pos >= 0);       

        if (this.available() < numOfBytes) {
            const newBuffer = Buffer.allocUnsafe(pos + numOfBytes);
            ObjectDataInput.buffer.copy(newBuffer, 0, 0, this.pos);
            ObjectDataInput.buffer = newBuffer;
        }

        assert(pos + numOfBytes <= ObjectDataInput.buffer.length);
    }

    private addOrUndefined(base: number, adder: number): number {
        if (base === undefined) {
            return undefined;
        } else {
            return base + adder;
        }
    }
}
