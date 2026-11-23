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
package com.lmax.disruptor.sequenced;

import com.lmax.disruptor.BusySpinProducerWaitStrategy;
import com.lmax.disruptor.LowLatencyProducerWaitStrategy;
import com.lmax.disruptor.MultiProducerSequencer;
import com.lmax.disruptor.ProducerWaitStrategy;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.util.Util;
import org.HdrHistogram.Histogram;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Measures producer latency and CPU usage while a bounded ring buffer is kept full.
 */
public final class ProducerBackpressureLatencyTest
{
    private static final int BUFFER_SIZE = 1024;
    private static final int CONSUMER_DELAY_NANOS = 5_000;
    private static final int MAX_PRODUCERS = 8;
    private static final int RUNS = 3;
    private static final long WARMUP_EVENTS = 100_000L;

    private ProducerBackpressureLatencyTest()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        final long events = args.length > 0 ? Long.parseLong(args[0]) : 1_000_000L;
        final int producerCount = Math.min(
            MAX_PRODUCERS, Math.max(1, Runtime.getRuntime().availableProcessors()));

        System.out.printf(
            "Producer backpressure test: producers=%d events=%d bufferSize=%d%n",
            producerCount, events, BUFFER_SIZE);
        run("low-latency", new LowLatencyProducerWaitStrategy(), producerCount, WARMUP_EVENTS, events);
        run("legacy-park", ProducerBackpressureLatencyTest::legacyPark, producerCount, WARMUP_EVENTS, events);
        run("busy-spin", new BusySpinProducerWaitStrategy(), producerCount, WARMUP_EVENTS, events);
    }

    private static long legacyPark(
        final long sequence, final long cursorSequence, final Sequence[] gatingSequences)
    {
        long gatingSequence;
        while (sequence > (gatingSequence = Util.getMinimumSequence(gatingSequences, cursorSequence)))
        {
            LockSupport.parkNanos(1L);
        }

        return gatingSequence;
    }

    private static void run(
        final String name,
        final ProducerWaitStrategy producerWaitStrategy,
        final int producerCount,
        final long warmupEvents,
        final long events) throws Exception
    {
        for (int i = 0; i < RUNS; i++)
        {
            final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 4);
            final Result result = runOnce(
                producerWaitStrategy, producerCount, warmupEvents, events, histogram);

            System.out.printf(
                "%s run %d: p50=%.1fus p99=%.1fus p999=%.1fus max=%.1fus cpu=%.1f%%%n",
                name,
                i,
                result.percentile(histogram, 50.0),
                result.percentile(histogram, 99.0),
                result.percentile(histogram, 99.9),
                result.percentile(histogram, 100.0),
                result.cpuPercent);
        }
    }

    @SuppressWarnings("deprecation")
    private static Result runOnce(
        final ProducerWaitStrategy producerWaitStrategy,
        final int producerCount,
        final long warmupEvents,
        final long events,
        final Histogram histogram) throws Exception
    {
        final MultiProducerSequencer sequencer = new MultiProducerSequencer(
            BUFFER_SIZE, new YieldingWaitStrategy(), producerWaitStrategy);
        final Sequence gatingSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
        sequencer.addGatingSequences(gatingSequence);

        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch producersReady = new CountDownLatch(producerCount);
        final CountDownLatch producersDone = new CountDownLatch(producerCount);
        final CountDownLatch releaseProducers = new CountDownLatch(1);
        final List<Thread> producers = new ArrayList<>();

        for (int i = 0; i < producerCount; i++)
        {
            final Thread producer = new Thread(
                () -> publish(
                    sequencer, producersReady, producersDone, releaseProducers,
                    warmupEvents, events, histogram),
                "producer-" + i);
            producer.setDaemon(true);
            producers.add(producer);
            producer.start();
        }

        producersReady.await();
        final Thread consumer = new Thread(
            () -> consume(gatingSequence, running, producerCount, warmupEvents + events),
            "consumer");
        consumer.setDaemon(true);
        consumer.start();

        final long[] producerIds = producers.stream().mapToLong(Thread::getId).toArray();
        final long cpuStart = producerCpuTime(producerIds);
        final long start = System.nanoTime();

        producersDone.await();
        final long elapsed = System.nanoTime() - start;
        final long cpuEnd = producerCpuTime(producerIds);
        running.set(false);
        releaseProducers.countDown();
        consumer.join();
        for (Thread producer : producers)
        {
            producer.join();
        }

        final double cpuPercent = 100.0 * (cpuEnd - cpuStart) / elapsed;
        return new Result(cpuPercent);
    }

    private static void publish(
        final MultiProducerSequencer sequencer,
        final CountDownLatch producersReady,
        final CountDownLatch producersDone,
        final CountDownLatch releaseProducers,
        final long warmupEvents,
        final long events,
        final Histogram histogram)
    {
        producersReady.countDown();
        try
        {
            producersReady.await();
            for (long i = 0; i < warmupEvents + events; i++)
            {
                final long start = System.nanoTime();
                final long sequence = sequencer.next();
                sequencer.publish(sequence);
                if (i >= warmupEvents)
                {
                    histogram.recordValue(System.nanoTime() - start);
                }
            }
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        finally
        {
            producersDone.countDown();
        }

        try
        {
            releaseProducers.await();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private static void consume(
        final Sequence gatingSequence,
        final AtomicBoolean running,
        final int producerCount,
        final long events)
    {
        long sequence = Sequencer.INITIAL_CURSOR_VALUE;
        while (running.get() && sequence < events * producerCount)
        {
            gatingSequence.set(++sequence);
            Thread.yield();
            long start = System.nanoTime();
            while (System.nanoTime() - start < CONSUMER_DELAY_NANOS)
            {
                Thread.onSpinWait();
            }
        }
    }

    private static long producerCpuTime(final long[] threadIds)
    {
        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long cpuTime = 0L;
        for (long threadId : threadIds)
        {
            final long threadCpuTime = bean.getThreadCpuTime(threadId);
            if (threadCpuTime >= 0)
            {
                cpuTime += threadCpuTime;
            }
        }
        return cpuTime;
    }

    private static final class Result
    {
        private final double cpuPercent;

        private Result(final double cpuPercent)
        {
            this.cpuPercent = cpuPercent;
        }

        private double percentile(final Histogram histogram, final double percentile)
        {
            return histogram.getValueAtPercentile(percentile) / 1_000.0;
        }
    }
}
