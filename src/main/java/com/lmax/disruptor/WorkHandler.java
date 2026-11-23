/*
 * Copyright 2011 LMAX Ltd.
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
package com.lmax.disruptor;

/**
 * Callback interface for handlers that consume work from a {@link WorkerPool}.
 *
 * <p>Each event is delivered to exactly one work handler. An individual work handler receives
 * events in sequence order.</p>
 *
 * @param <T> event implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public interface WorkHandler<T>
{
    /**
     * Called when an event has been assigned to this work handler.
     *
     * @param event published to the {@link RingBuffer}
     * @throws Exception if the work handler would like the exception handled further up the chain.
     */
    void onEvent(T event) throws Exception;
}
