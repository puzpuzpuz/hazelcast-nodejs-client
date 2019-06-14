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
import {SetAddAllCodec} from '../codec/SetAddAllCodec';
import {SetAddCodec} from '../codec/SetAddCodec';
import {SetAddListenerCodec} from '../codec/SetAddListenerCodec';
import {SetClearCodec} from '../codec/SetClearCodec';
import {SetCompareAndRemoveAllCodec} from '../codec/SetCompareAndRemoveAllCodec';
import {SetCompareAndRetainAllCodec} from '../codec/SetCompareAndRetainAllCodec';
import {SetContainsAllCodec} from '../codec/SetContainsAllCodec';
import {SetContainsCodec} from '../codec/SetContainsCodec';
import {SetGetAllCodec} from '../codec/SetGetAllCodec';
import {SetIsEmptyCodec} from '../codec/SetIsEmptyCodec';
import {SetRemoveCodec} from '../codec/SetRemoveCodec';
import {SetRemoveListenerCodec} from '../codec/SetRemoveListenerCodec';
import {SetSizeCodec} from '../codec/SetSizeCodec';
import {ItemEvent, ItemEventType, ItemListener} from '../core/ItemListener';
import {ListenerMessageCodec} from '../ListenerMessageCodec';
import {Data} from '../serialization/Data';
import {ISet} from './ISet';
import {PartitionSpecificProxy} from './PartitionSpecificProxy';
import {ClientInputMessage, ClientOutputMessage} from '../ClientMessage';

export class SetProxy<E> extends PartitionSpecificProxy implements ISet<E> {

    add(entry: E): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetAddCodec, this.toData(entry));
    }

    addAll(items: E[]): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetAddAllCodec, this.serializeList(items));
    }

    toArray(): Promise<E[]> {
        return this.encodeInvoke(SetGetAllCodec).then((items: Data[]) => {
            return items.map((item) => {
                return this.toObject(item);
            });
        });
    }

    clear(): Promise<void> {
        return this.encodeInvoke<void>(SetClearCodec);
    }

    contains(entry: E): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetContainsCodec, this.toData(entry));
    }

    containsAll(items: E[]): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetContainsAllCodec, this.serializeList(items));
    }

    isEmpty(): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetIsEmptyCodec);
    }

    remove(entry: E): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetRemoveCodec, this.toData(entry));
    }

    removeAll(items: E[]): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetCompareAndRemoveAllCodec, this.serializeList(items));
    }

    retainAll(items: E[]): Promise<boolean> {
        return this.encodeInvoke<boolean>(SetCompareAndRetainAllCodec, this.serializeList(items));
    }

    size(): Promise<number> {
        return this.encodeInvoke<number>(SetSizeCodec);
    }

    addItemListener(listener: ItemListener<E>, includeValue: boolean = true): Promise<string> {
        const handler = (message: ClientInputMessage) => {
            SetAddListenerCodec.handle(message, (item: Data, uuid: string, eventType: number) => {
                const responseObject = this.toObject(item);
                const member = this.client.getClusterService().getMember(uuid);
                const name = this.name;
                const itemEvent = new ItemEvent(name, eventType, responseObject, member);

                if (eventType === ItemEventType.ADDED && listener.itemAdded) {
                    listener.itemAdded.apply(null, [itemEvent]);
                } else if (eventType === ItemEventType.REMOVED && listener.itemRemoved) {
                    listener.itemRemoved.apply(null, [itemEvent]);
                }
            });
        };
        const codec = this.createEntryListener(this.name, includeValue);
        return this.client.getListenerService().registerListener(codec, handler);
    }

    removeItemListener(registrationId: string): Promise<boolean> {
        return this.client.getListenerService().deregisterListener(registrationId);
    }

    private serializeList(input: any[]): Data[] {
        return input.map((each) => {
            return this.toData(each);
        });
    }

    private createEntryListener(name: string, includeValue: boolean): ListenerMessageCodec {
        return {
            encodeAddRequest(localOnly: boolean): ClientOutputMessage {
                return SetAddListenerCodec.encodeRequest(name, includeValue, localOnly);
            },
            decodeAddResponse(msg: ClientInputMessage): string {
                return SetAddListenerCodec.decodeResponse(msg).response;
            },
            encodeRemoveRequest(listenerId: string): ClientOutputMessage {
                return SetRemoveListenerCodec.encodeRequest(name, listenerId);
            },
        };
    }
}
