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

package edu.harvard.s3.task;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Process task queue to execute queue of tasks with provided parallelism.
 */
@Slf4j
public class IteratorTaskProcessor<T extends ProcessTask> {

    private final long parallelism;

    private final Iterator<T> iterator;

    private final Callback callback;

    private final ExecutorService executor;

    /**
     * Process task queue constructor.
     *
     * @param parallelism parallelism desired for queue processing
     * @param iterator    iterator of process tasks
     * @param callback    callback for when queue completes
     */
    public IteratorTaskProcessor(int parallelism, Iterator<T> iterator, Callback callback) {
        this.parallelism = parallelism;
        this.iterator = iterator;
        this.callback = callback;
        this.executor = newFixedThreadPool(parallelism);
    }

    /**
     * Start iterator task processor.
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
        log.info("submitting task {}", task.id());
        CompletableFuture.supplyAsync(() -> task.execute(), executor)
            .thenAccept(t -> {
                try {
                    complete(t);
                } catch (InterruptedException e) {
                    log.error("failed to complete task", e);
                }
            });
    }

    private synchronized void complete(ProcessTask task) throws InterruptedException {
        log.info("task {} complete", task.id());
        task.complete();
        if (this.iterator.hasNext()) {
            submit(this.iterator.next());
        } else {
            shutdown();
        }
    }

    private void shutdown() throws InterruptedException {
        log.info("shutting down process task queue");
        executor.shutdown();
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {}
        this.callback.complete();
    }

}
