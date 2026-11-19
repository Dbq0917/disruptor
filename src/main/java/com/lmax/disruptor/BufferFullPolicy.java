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
 * Strategy employed by a {@link RingBuffer} when a producer attempts to publish
 * to a ring buffer that has no remaining capacity.
 *
 * <p>The policy is fixed when the ring buffer is created and applies to the
 * {@code publishEvent(s)} methods of {@link RingBuffer}. The raw claim/publish
 * API ({@link RingBuffer#next()} followed by {@link RingBuffer#publish(long)})
 * always blocks when the buffer is full, as a claimed sequence must be published.</p>
 */
public enum BufferFullPolicy
{
    /**
     * The producer blocks until the consumers have freed up enough capacity.
     * No events are lost, but producer throughput is limited by the slowest consumer.
     * This is the default and matches the historical behaviour of the disruptor.
     */
    BLOCK,

    /**
     * The producer never blocks. Events that do not fit in the remaining capacity are
     * dropped and counted, preserving producer throughput at the cost of losing data.
     * The number of dropped events is available via {@link RingBuffer#getDroppedEventCount()}.
     */
    DROP
}
