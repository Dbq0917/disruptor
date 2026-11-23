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

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pool of {@link WorkProcessor}s that consume events from a shared queue.
 *
 * <p>Events are assigned to workers through a shared work sequence, so each event is processed by
 * exactly one worker. Each worker keeps an independent sequence for its own progress, which allows
 * a fast worker to continue claiming events while another worker is still processing an earlier
 * event.</p>
 *
 * <p>Once a WorkerPool has been halted it cannot be started again.</p>
 *
 * @param <T> event to be processed by a pool of workers.
 */
public final class WorkerPool<T>
{
    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int IDLE = 0;
    private static final int RUNNING = IDLE + 1;
    private static final int HALTED = RUNNING + 1;

    private final AtomicInteger runState = new AtomicInteger(IDLE);
    private final Sequence workSequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private final RingBuffer<T> ringBuffer;
    private final WorkProcessor<?>[] workProcessors;

    /**
     * Create a worker pool to distribute published events across a set of work handlers.
     *
     * <p>This option requires a pre-configured {@link RingBuffer} which must have
     * {@link RingBuffer#addGatingSequences(Sequence...)} called before the worker pool is started.</p>
     *
     * @param ringBuffer       of events to be consumed.
     * @param sequenceBarrier  on which the workers will depend.
     * @param exceptionHandler to callback when an error occurs which is not handled by the work handlers.
     * @param workHandlers     to distribute the work load across.
     */
    @SafeVarargs
    public WorkerPool(
        final RingBuffer<T> ringBuffer,
        final SequenceBarrier sequenceBarrier,
        final ExceptionHandler<? super T> exceptionHandler,
        final WorkHandler<? super T>... workHandlers)
    {
        if (workHandlers.length == 0)
        {
            throw new IllegalArgumentException("At least one work handler is required");
        }

        this.ringBuffer = ringBuffer;
        workProcessors = new WorkProcessor<?>[workHandlers.length];

        for (int i = 0; i < workHandlers.length; i++)
        {
            workProcessors[i] = new WorkProcessor<>(
                ringBuffer,
                sequenceBarrier,
                workHandlers[i],
                exceptionHandler,
                workSequence);
        }
    }

    /**
     * Construct a work pool with an internal ring buffer for convenience.
     *
     * <p>This option does not require {@link RingBuffer#addGatingSequences(Sequence...)} to be called
     * before the work pool is started.</p>
     *
     * @param eventFactory     for filling the {@link RingBuffer}.
     * @param exceptionHandler to callback when an error occurs which is not handled by the work handlers.
     * @param workHandlers     to distribute the work load across.
     */
    @SafeVarargs
    public WorkerPool(
        final EventFactory<T> eventFactory,
        final ExceptionHandler<? super T> exceptionHandler,
        final WorkHandler<? super T>... workHandlers)
    {
        if (workHandlers.length == 0)
        {
            throw new IllegalArgumentException("At least one work handler is required");
        }

        ringBuffer = RingBuffer.createMultiProducer(eventFactory, DEFAULT_BUFFER_SIZE, new BlockingWaitStrategy());
        final SequenceBarrier barrier = ringBuffer.newBarrier();
        workProcessors = new WorkProcessor<?>[workHandlers.length];
        for (int i = 0; i < workHandlers.length; i++)
        {
            workProcessors[i] = new WorkProcessor<>(
                ringBuffer,
                barrier,
                workHandlers[i],
                exceptionHandler,
                workSequence);
        }

        ringBuffer.addGatingSequences(getWorkerSequences());
    }

    /**
     * Get sequences representing the progress of the workers and the shared work assignment.
     *
     * @return the worker sequences followed by the shared work sequence.
     */
    public Sequence[] getWorkerSequences()
    {
        final Sequence[] sequences = new Sequence[workProcessors.length + 1];
        for (int i = 0; i < workProcessors.length; i++)
        {
            sequences[i] = workProcessors[i].getSequence();
        }
        sequences[sequences.length - 1] = workSequence;

        return sequences;
    }

    /**
     * Start the worker pool using the supplied executor.
     *
     * @param executor providing threads for running the workers.
     * @return the {@link RingBuffer} used for the work queue.
     * @throws IllegalStateException if this pool is anything other than idle.
     */
    public RingBuffer<T> start(final Executor executor)
    {
        prepareToStart();
        for (final WorkProcessor<?> processor : workProcessors)
        {
            executor.execute(processor);
        }

        return ringBuffer;
    }

    /**
     * Start the worker pool using the supplied thread factory.
     *
     * @param threadFactory used to create one thread per worker.
     * @return the {@link RingBuffer} used for the work queue.
     * @throws IllegalStateException if this pool is anything other than idle.
     */
    public RingBuffer<T> start(final ThreadFactory threadFactory)
    {
        prepareToStart();
        for (final WorkProcessor<?> processor : workProcessors)
        {
            final Thread thread = threadFactory.newThread(processor);
            if (thread == null)
            {
                throw new IllegalStateException("Failed to create thread to run work processor");
            }
            thread.start();
        }

        return ringBuffer;
    }

    /**
     * Wait for the ring buffer to drain of published events and then halt the workers.
     */
    public void drainAndHalt()
    {
        final Sequence[] workerSequences = getWorkerSequences();
        while (ringBuffer.getCursor() > Util.getMinimumSequence(workerSequences))
        {
            Thread.yield();
        }

        halt();
    }

    /**
     * Halt all workers immediately at the end of their current cycle.
     */
    public void halt()
    {
        for (final WorkProcessor<?> processor : workProcessors)
        {
            processor.halt();
        }

        runState.set(HALTED);
    }

    /**
     * Check whether the worker pool is running.
     *
     * @return true if the pool has been started and has not been halted.
     */
    public boolean isRunning()
    {
        return runState.get() == RUNNING;
    }

    private void prepareToStart()
    {
        final int currentState = runState.get();
        if (currentState == RUNNING)
        {
            throw new IllegalStateException("WorkerPool has already been started and cannot be restarted until halted.");
        }
        if (currentState == HALTED)
        {
            throw new IllegalStateException("Cannot run a WorkerPool that has been halted");
        }
        if (!runState.compareAndSet(IDLE, RUNNING))
        {
            throw new IllegalStateException("WorkerPool has already been started and cannot be restarted until halted.");
        }
    }
}
