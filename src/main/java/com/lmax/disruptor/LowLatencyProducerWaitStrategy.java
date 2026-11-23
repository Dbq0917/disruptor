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

import java.util.concurrent.locks.LockSupport;

/**
 * Producer wait strategy that keeps short waits in user space while releasing the CPU during
 * sustained backpressure.
 *
 * <p>It spins briefly, yields for a small number of attempts, then parks for a bounded number
 * of nanoseconds. The park timeout is deliberately limited to keep wake-up latency in the
 * microsecond range instead of allowing it to drift into milliseconds.</p>
 */
public final class LowLatencyProducerWaitStrategy implements ProducerWaitStrategy
{
    private static final int DEFAULT_SPIN_RETRIES = 100;
    private static final int DEFAULT_YIELD_RETRIES = 10;
    private static final long DEFAULT_PARK_NANOS = 100_000L;

    private final int spinRetries;
    private final int yieldRetries;
    private final long parkNanos;

    /**
     * Create a strategy using the default low-latency backoff settings.
     */
    public LowLatencyProducerWaitStrategy()
    {
        this(DEFAULT_SPIN_RETRIES, DEFAULT_YIELD_RETRIES, DEFAULT_PARK_NANOS);
    }

    /**
     * Create a strategy with explicit backoff settings.
     *
     * @param spinRetries  number of initial spin attempts.
     * @param yieldRetries number of yield attempts after the initial spin.
     * @param parkNanos    time to park on each subsequent attempt.
     */
    public LowLatencyProducerWaitStrategy(final int spinRetries, final int yieldRetries, final long parkNanos)
    {
        if (spinRetries < 0)
        {
            throw new IllegalArgumentException("spinRetries must not be negative");
        }
        if (yieldRetries < 0)
        {
            throw new IllegalArgumentException("yieldRetries must not be negative");
        }
        if (parkNanos < 0)
        {
            throw new IllegalArgumentException("parkNanos must not be negative");
        }

        this.spinRetries = spinRetries;
        this.yieldRetries = yieldRetries;
        this.parkNanos = parkNanos;
    }

    @Override
    public long waitForCapacity(final long sequence, final long cursorSequence, final Sequence[] gatingSequences)
    {
        int retries = spinRetries + yieldRetries;
        long gatingSequence;

        while (sequence > (gatingSequence = Util.getMinimumSequence(gatingSequences, cursorSequence)))
        {
            if (retries > yieldRetries)
            {
                Thread.onSpinWait();
            }
            else if (retries > 0)
            {
                Thread.yield();
            }
            else
            {
                LockSupport.parkNanos(parkNanos);
            }

            if (retries > 0)
            {
                retries--;
            }
        }

        return gatingSequence;
    }

    @Override
    public String toString()
    {
        return "LowLatencyProducerWaitStrategy{" +
            "spinRetries=" + spinRetries +
            ", yieldRetries=" + yieldRetries +
            ", parkNanos=" + parkNanos +
            '}';
    }
}
