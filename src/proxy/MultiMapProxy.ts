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

import * as Promise from 'bluebird';

import * as Long from 'long';
import {MultiMapForceUnlockCodec} from '../codec/MultiMapForceUnlockCodec';
import {MultiMapIsLockedCodec} from '../codec/MultiMapIsLockedCodec';
import {MultiMapLockCodec} from '../codec/MultiMapLockCodec';
import {MultiMapTryLockCodec} from '../codec/MultiMapTryLockCodec';
import {MultiMapUnlockCodec} from '../codec/MultiMapUnlockCodec';
import {EventType} from '../core/EventType';
import {EntryEvent, EntryListener} from '../core/EntryListener';
import {ReadOnlyLazyList} from '../core/ReadOnlyLazyList';
import {ListenerMessageCodec} from '../ListenerMessageCodec';
import {LockReferenceIdGenerator} from '../LockReferenceIdGenerator';
import {Data} from '../serialization/Data';
import {MultiMapAddEntryListenerCodec} from './../codec/MultiMapAddEntryListenerCodec';
import {MultiMapAddEntryListenerToKeyCodec} from './../codec/MultiMapAddEntryListenerToKeyCodec';
import {MultiMapClearCodec} from './../codec/MultiMapClearCodec';
import {MultiMapContainsEntryCodec} from './../codec/MultiMapContainsEntryCodec';
import {MultiMapContainsKeyCodec} from './../codec/MultiMapContainsKeyCodec';
import {MultiMapContainsValueCodec} from './../codec/MultiMapContainsValueCodec';
import {MultiMapEntrySetCodec} from './../codec/MultiMapEntrySetCodec';
import {MultiMapGetCodec} from './../codec/MultiMapGetCodec';
import {MultiMapKeySetCodec} from './../codec/MultiMapKeySetCodec';
import {MultiMapPutCodec} from './../codec/MultiMapPutCodec';
import {MultiMapRemoveCodec} from './../codec/MultiMapRemoveCodec';
import {MultiMapRemoveEntryCodec} from './../codec/MultiMapRemoveEntryCodec';
import {MultiMapRemoveEntryListenerCodec} from './../codec/MultiMapRemoveEntryListenerCodec';
import {MultiMapSizeCodec} from './../codec/MultiMapSizeCodec';
import {MultiMapValueCountCodec} from './../codec/MultiMapValueCountCodec';
import {MultiMapValuesCodec} from './../codec/MultiMapValuesCodec';
import {BaseProxy} from './BaseProxy';
import {MultiMap} from './MultiMap';
import {ClientInputMessage, ClientOutputMessage} from '../ClientMessage';
import {MapEvent} from '../core/MapListener';

export class MultiMapProxy<K, V> extends BaseProxy implements MultiMap<K, V> {

    private lockReferenceIdGenerator: LockReferenceIdGenerator = this.client.getLockReferenceIdGenerator();
    private deserializeList = <X>(items: Data[]): X[] => {
        return items.map<X>(this.toObject.bind(this));
        // tslint:disable-next-line:semicolon
    };

    /*tslint:disable:member-ordering*/
    put(key: K, value: V): Promise<boolean> {
        const keyData = this.toData(key);
        const valueData = this.toData(value);
        return this.encodeInvokeOnKey<boolean>(MultiMapPutCodec, keyData, keyData, valueData, 1);
    }

    get(key: K): Promise<ReadOnlyLazyList<V>> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<Data[]>(MultiMapGetCodec, keyData, keyData, 1).then((data: Data[]) => {
            return new ReadOnlyLazyList<V>(data, this.client.getSerializationService());
        });
    }

    remove(key: K, value: V): Promise<boolean> {
        const keyData = this.toData(key);
        const valueData = this.toData(value);
        return this.encodeInvokeOnKey<boolean>(MultiMapRemoveEntryCodec, keyData, keyData, valueData, 1);
    }

    removeAll(key: K): Promise<ReadOnlyLazyList<V>> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<Data[]>(MultiMapRemoveCodec, keyData, keyData, 1).then((data: Data[]) => {
            return new ReadOnlyLazyList<V>(data, this.client.getSerializationService());
        });
    }

    keySet(): Promise<K[]> {
        return this.encodeInvokeOnRandomTarget<Data[]>(MultiMapKeySetCodec)
            .then<K[]>(this.deserializeList);
    }

    values(): Promise<ReadOnlyLazyList<V>> {
        return this.encodeInvokeOnRandomTarget<Data[]>(MultiMapValuesCodec).then((data: Data[]) => {
            return new ReadOnlyLazyList<V>(data, this.client.getSerializationService());
        });
    }

    entrySet(): Promise<Array<[K, V]>> {
        return this.encodeInvokeOnRandomTarget<Array<[Data, Data]>>(MultiMapEntrySetCodec)
            .then<Array<[K, V]>>((entrySet: Array<[Data, Data]>) => {
                return entrySet.map((entry: any[]) => {
                    return [this.toObject(entry[0]), this.toObject(entry[1])] as [K, V];
                });
            });
    }

    containsKey(key: K): Promise<boolean> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<boolean>(MultiMapContainsKeyCodec, keyData, keyData, 1);
    }

    containsValue(value: V): Promise<boolean> {
        const valueData = this.toData(value);
        return this.encodeInvokeOnRandomTarget<boolean>(MultiMapContainsValueCodec, valueData);
    }

    containsEntry(key: K, value: V): Promise<boolean> {
        const keyData = this.toData(key);
        const valueData = this.toData(value);
        return this.encodeInvokeOnKey<boolean>(MultiMapContainsEntryCodec, keyData, keyData, valueData, 1);
    }

    size(): Promise<number> {
        return this.encodeInvokeOnRandomTarget<number>(MultiMapSizeCodec);
    }

    clear(): Promise<void> {
        return this.encodeInvokeOnRandomTarget<void>(MultiMapClearCodec);
    }

    valueCount(key: K): Promise<number> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<number>(MultiMapValueCountCodec, keyData, keyData, 1);
    }

    addEntryListener(listener: EntryListener<K, V>, key?: K, includeValue: boolean = true): Promise<string> {
        const toObject = this.toObject.bind(this);

        /* tslint:disable: no-shadowed-variable */
        const entryEventHandler = (key: K, value: V, oldValue: V, mergingValue: V, eventType: number, uuid: string,
                                   numberOfAffectedEntries: number) => {
            const member = this.client.getClusterService().getMember(uuid);
            const name = this.name;

            key = toObject(key);
            value = toObject(value);
            oldValue = toObject(oldValue);
            mergingValue = toObject(mergingValue);

            const entryEvent = new EntryEvent(name, key, value, oldValue, mergingValue, member);

            const mapEvent = new MapEvent(name, numberOfAffectedEntries, member);

            // Multi map only supports these three event types
            switch (eventType) {
                case EventType.ADDED:
                    if (listener.added) {
                        listener.added.apply(null, [entryEvent]);
                    }
                    break;
                case EventType.REMOVED:
                    if (listener.removed) {
                        listener.removed.apply(null, [entryEvent]);
                    }
                    break;
                case EventType.CLEAR_ALL:
                    if (listener.mapCleared) {
                        listener.mapCleared.apply(null, [mapEvent]);
                    }
                    break;
            }
        };

        if (key) {
            const keyData = this.toData(key);
            const handler = (m: ClientInputMessage) => {
                MultiMapAddEntryListenerToKeyCodec.handle(m, entryEventHandler, toObject);
            };
            const codec = this.createEntryListenerToKey(this.name, keyData, includeValue);

            return this.client.getListenerService().registerListener(codec, handler);
        } else {
            const listenerHandler = (m: ClientInputMessage) => {
                MultiMapAddEntryListenerCodec.handle(m, entryEventHandler, toObject);
            };
            const codec = this.createEntryListener(this.name, includeValue);

            return this.client.getListenerService().registerListener(codec, listenerHandler);
        }
    }

    removeEntryListener(listenerId: string): Promise<boolean> {
        return this.client.getListenerService().deregisterListener(listenerId);
    }

    lock(key: K, leaseMillis: number = -1): Promise<void> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<void>(MultiMapLockCodec, keyData, keyData, 1, leaseMillis, this.nextSequence());
    }

    isLocked(key: K): Promise<boolean> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<boolean>(MultiMapIsLockedCodec, keyData, keyData);
    }

    tryLock(key: K, timeoutMillis: number = 0, leaseMillis: number = -1): Promise<boolean> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<boolean>(MultiMapTryLockCodec, keyData, keyData, 1, leaseMillis,
            timeoutMillis, this.nextSequence());
    }

    unlock(key: K): Promise<void> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<void>(MultiMapUnlockCodec, keyData, keyData, 1, this.nextSequence());
    }

    forceUnlock(key: K): Promise<void> {
        const keyData = this.toData(key);
        return this.encodeInvokeOnKey<void>(MultiMapForceUnlockCodec, keyData, keyData, this.nextSequence());
    }

    private nextSequence(): Long {
        return this.lockReferenceIdGenerator.getNextReferenceId();
    }

    private createEntryListenerToKey(name: string, keyData: Data, includeValue: boolean): ListenerMessageCodec {
        return {
            encodeAddRequest(localOnly: boolean): ClientOutputMessage {
                return MultiMapAddEntryListenerToKeyCodec.encodeRequest(name, keyData, includeValue, localOnly);
            },
            decodeAddResponse(msg: ClientInputMessage): string {
                return MultiMapAddEntryListenerToKeyCodec.decodeResponse(msg).response;
            },
            encodeRemoveRequest(listenerId: string): ClientOutputMessage {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, listenerId);
            },
        };
    }

    private createEntryListener(name: string, includeValue: boolean): ListenerMessageCodec {
        return {
            encodeAddRequest(localOnly: boolean): ClientOutputMessage {
                return MultiMapAddEntryListenerCodec.encodeRequest(name, includeValue, localOnly);
            },
            decodeAddResponse(msg: ClientInputMessage): string {
                return MultiMapAddEntryListenerCodec.decodeResponse(msg).response;
            },
            encodeRemoveRequest(listenerId: string): ClientOutputMessage {
                return MultiMapRemoveEntryListenerCodec.encodeRequest(name, listenerId);
            },
        };
    }
}
