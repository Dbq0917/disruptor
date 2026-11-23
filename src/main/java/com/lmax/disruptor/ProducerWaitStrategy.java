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
 * Strategy employed by a producer when a ring buffer does not have enough capacity to claim
 * the requested sequence range.
 *
 * <p>Implementations should return once one of the supplied gating sequences has advanced far
 * enough to make the requested range available. While waiting they should avoid unbounded
 * busy-spin so that other work in the same process can be scheduled.</p>
 */
@FunctionalInterface
public interface ProducerWaitStrategy
{
    /**
     * Wait until the required capacity is available.
     *
     * @param sequence         the minimum gating sequence required to make the claim possible.
     * @param cursorSequence   the producer cursor value before the claim was attempted.
     * @param gatingSequences  sequences gating the producer.
     * @return the minimum gating sequence which made the capacity available.
     */
    long waitForCapacity(long sequence, long cursorSequence, Sequence[] gatingSequences);
}
