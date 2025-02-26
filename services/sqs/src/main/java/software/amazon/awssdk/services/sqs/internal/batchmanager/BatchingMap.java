/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.sqs.internal.batchmanager;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Outer map maps a batchKey (ex. queueUrl, overrideConfig etc.) to a {@link RequestBatchBuffer}
 *
 * @param <RequestT> the type of an outgoing response
 */

@SdkInternalApi
public final class BatchingMap<RequestT, ResponseT> {
    private final int maxBatchKeys;
    private final int maxBufferSize;
    private final Map<String, RequestBatchBuffer<RequestT, ResponseT>> buffersByKey;
    private final BatchRequestExtractor<RequestT, ResponseT> requestExtractor;

    /**
     * Creates a new batch buffer manager.
     *
     * @param overrideConfiguration The batch configuration
     */
    public BatchingMap(RequestBatchConfiguration overrideConfiguration) {
        this.buffersByKey = new ConcurrentHashMap<>();
        this.maxBatchKeys = overrideConfiguration.maxBatchKeys();
        this.maxBufferSize = overrideConfiguration.maxBufferSize();

        FlushPolicy<RequestT, ResponseT> flushPolicy = new FlushPolicy<>(
            overrideConfiguration.maxBatchItems(),
            overrideConfiguration.maxBatchBytesSize()
        );
        this.requestExtractor = new BatchRequestExtractor<>(flushPolicy);
    }

    /**
     * Adds a request to the appropriate buffer by batch key.
     *
     * @param batchKey The key identifying which batch this request belongs to
     * @param scheduleFlush Supplier that creates a scheduled flush task
     * @param request The request to add
     * @param response The future that will receive the response
     * @throws IllegalStateException if the maximum number of batch keys is exceeded
     */
    public void addRequest(String batchKey, Supplier<ScheduledFuture<?>> scheduleFlush,
                           RequestT request, CompletableFuture<ResponseT> response) {
        buffersByKey.computeIfAbsent(batchKey, k -> {
            if (buffersByKey.size() == maxBatchKeys) {
                throw new IllegalStateException("Reached MaxBatchKeys of: " + maxBatchKeys);
            }
            return new RequestBatchBuffer<>(scheduleFlush.get(), maxBufferSize);
        }).put(request, response);
    }

    /**
     * Checks if a buffer exists for the given batch key.
     *
     * @param batchKey The batch key to check
     * @return true if a buffer exists for this key
     */
    public boolean containsKey(String batchKey) {
        return buffersByKey.containsKey(batchKey);
    }

    /**
     * Gets requests that should be flushed based on current buffer state.
     *
     * @param batchKey The batch key to check
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> getRequestsToFlush(String batchKey) {
        return requestExtractor.extractFlushableRequests(buffersByKey.get(batchKey));
    }

    /**
     * Gets requests that should be flushed before adding a new request.
     *
     * @param batchKey The batch key to check
     * @param request The new request that would be added
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> getRequestsToFlushBeforeAdd(
        String batchKey, RequestT request) {
        return requestExtractor.extractFlushableRequestsBeforeAdd(buffersByKey.get(batchKey), request);
    }

    /**
     * Gets requests that should be flushed during a scheduled flush.
     *
     * @param batchKey The batch key to check
     * @param maxBatchItems Maximum number of items to extract
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> getScheduledRequestsToFlush(
        String batchKey, int maxBatchItems) {
        return requestExtractor.extractScheduledFlushableRequests(buffersByKey.get(batchKey), maxBatchItems);
    }

    /**
     * Updates the scheduled flush task for a buffer.
     *
     * @param batchKey The batch key identifying the buffer
     * @param scheduledFlush The new scheduled flush task
     */
    public void updateScheduledFlush(String batchKey, ScheduledFuture<?> scheduledFlush) {
        RequestBatchBuffer<RequestT, ResponseT> buffer = buffersByKey.get(batchKey);
        if (buffer != null) {
            buffer.putScheduledFlush(scheduledFlush);
        }
    }

    /**
     * Cancels the scheduled flush task for a buffer.
     *
     * @param batchKey The batch key identifying the buffer
     */
    public void cancelScheduledFlush(String batchKey) {
        RequestBatchBuffer<RequestT, ResponseT> buffer = buffersByKey.get(batchKey);
        if (buffer != null) {
            buffer.cancelScheduledFlush();
        }
    }

    /**
     * Performs an action on each buffer.
     *
     * @param action The action to perform
     */
    public void forEach(BiConsumer<String, RequestBatchBuffer<RequestT, ResponseT>> action) {
        buffersByKey.forEach(action);
    }

    /**
     * Clears all buffers.
     */
    public void clear() {
        for (Map.Entry<String, RequestBatchBuffer<RequestT, ResponseT>> entry : buffersByKey.entrySet()) {
            entry.getValue().clear();
        }
        buffersByKey.clear();
    }
}
