/**
 * Copyright (c) 2021 President and Fellows of Harvard College
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.harvard.drs.remediation.task;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 * Concurrently process tasks at parallelism level until iterator completes.
 */
@Slf4j
public class IteratingTaskProcessor<T extends ProcessTask> {

    private final long parallelism;

    private final Iterator<T> iterator;

    private final Callback callback;

    private final ExecutorService executor;

    private final AtomicInteger count;

    private final AtomicInteger total;

    private final AtomicBoolean shuttingDown;

    /**
     * Iterating task processor constructor.
     *
     * @param parallelism parallelism desired for processing
     * @param iterator    iterator of process tasks
     * @param callback    callback for when iterator completes
     */
    public IteratingTaskProcessor(int parallelism, Iterator<T> iterator, Callback callback) {
        this.parallelism = parallelism;
        this.iterator = iterator;
        this.callback = callback;
        this.executor = newFixedThreadPool(parallelism);
        this.count = new AtomicInteger();
        this.total = new AtomicInteger();
        this.shuttingDown = new AtomicBoolean();
    }

    /**
     * Start iterating task processor.
     */
    public void start() {
        int i = 0;
        while (this.iterator.hasNext() && i++ < parallelism) {
            submit(this.iterator.next());
        }
    }

    /**
     * Submit task to executor service.
     *
     * @param task process task to submit to executor service
     */
    public void submit(ProcessTask task) {
        log.info("submitting task {}: {}", this.count.incrementAndGet(), task.id());
        CompletableFuture.supplyAsync(() -> task.execute(), executor)
            .thenAccept(t -> complete(t));
    }

    private void complete(ProcessTask task) {
        log.info("completing task {}: {} - {}", this.count.getAndDecrement(), task.id(), this.total.incrementAndGet());
        task.complete();
        if (this.iterator.hasNext()) {
            submit(this.iterator.next());
        } else {
            if (this.shuttingDown.compareAndSet(false, true)) {
                shutdown();
            }
        }
    }

    private void shutdown() {
        log.info("shutting down task processor waiting on {} tasks in progress", this.count.get());
        executor.shutdown();
        try {
            while (this.count.get() > 0 && !executor.awaitTermination(15, TimeUnit.SECONDS)) {}
        } catch (InterruptedException e) {
            log.error("Failed to await termination", e);
            executor.shutdownNow();
        }
        this.callback.complete();
    }

}
