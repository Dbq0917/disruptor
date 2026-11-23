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

import com.lmax.disruptor.util.Util;

/**
 * Producer wait strategy that busy-spins until capacity becomes available.
 *
 * <p>This provides the lowest possible wake-up latency but can consume a full CPU core per
 * waiting producer. It should only be used when producers are bound to dedicated cores.</p>
 */
public final class BusySpinProducerWaitStrategy implements ProducerWaitStrategy
{
    /**
     * Construct a busy-spin producer wait strategy.
     */
    public BusySpinProducerWaitStrategy()
    {
    }

    @Override
    public long waitForCapacity(final long sequence, final long cursorSequence, final Sequence[] gatingSequences)
    {
        long gatingSequence;

        while (sequence > (gatingSequence = Util.getMinimumSequence(gatingSequences, cursorSequence)))
        {
            Thread.onSpinWait();
        }

        return gatingSequence;
    }
}
