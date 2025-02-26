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


import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Stores and manages batch requests for a specific batch key.
 *
 * @param <RequestT> The type of request being batched
 * @param <ResponseT> The type of response for each request
 */
@SdkInternalApi
public final class RequestBatchBuffer<RequestT, ResponseT> {
    private final Object lock = new Object();

    private final Map<String, BatchingExecutionContext<RequestT, ResponseT>> idToBatchContext;
    private final int maxBufferSize;
    /**
     * Batch entries in a batch request require a unique ID so nextId keeps track of the ID to assign to the next
     * BatchingExecutionContext. For simplicity, the ID is just an integer that is incremented everytime a new request and
     * response pair is received.
     */
    private int nextId;
    /**
     * Keeps track of the ID of the next entry to be added in a batch request. This ID does not necessarily correlate to a request
     * that already exists in the idToBatchContext map since it refers to the next entry (ex. if the last entry added to
     * idToBatchContext had an id of 22, nextBatchEntry will have a value of 23).
     */
    private int nextBatchEntry;

    /**
     * The scheduled flush task associated with this batchBuffer.
     */
    private ScheduledFuture<?> scheduledFlush;

    /**
     * Creates a new request batch buffer.
     *
     * @param scheduledFlush The scheduled flush task
     * @param maxBufferSize Maximum number of entries this buffer can hold
     */
    public RequestBatchBuffer(ScheduledFuture<?> scheduledFlush, int maxBufferSize) {
        this.idToBatchContext = new ConcurrentHashMap<>();
        this.nextId = 0;
        this.nextBatchEntry = 0;
        this.scheduledFlush = scheduledFlush;
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Adds a request and its response future to the buffer.
     *
     * @param request The request to add
     * @param response The future that will receive the response
     * @throws IllegalStateException if the buffer is full
     */
    public void put(RequestT request, CompletableFuture<ResponseT> response) {
        synchronized (lock) {
            if (idToBatchContext.size() == maxBufferSize) {
                throw new IllegalStateException("Reached MaxBufferSize of: " + maxBufferSize);
            }

            if (nextId == Integer.MAX_VALUE) {
                nextId = 0;
            }
            String id = Integer.toString(nextId++);
            idToBatchContext.put(id, new BatchingExecutionContext<>(request, response));
        }
    }

    /**
     * Gets the current number of entries in the buffer.
     *
     * @return The number of entries
     */
    public int size() {
        return idToBatchContext.size();
    }

    /**
     * Checks if the buffer is empty.
     *
     * @return true if the buffer contains no entries
     */
    public boolean isEmpty() {
        return idToBatchContext.isEmpty();
    }

    /**
     * Gets the total size in bytes of all requests in the buffer.
     *
     * @return The total size in bytes
     */
    public int getCurrentBatchSizeInBytes() {
        return idToBatchContext.values().stream()
                               .map(BatchingExecutionContext::responsePayloadByteSize)
                               .mapToInt(opt -> opt.orElse(0))
                               .sum();
    }

    /**
     * Extracts a specified number of entries from the buffer.
     *
     * @param maxEntries Maximum number of entries to extract
     * @return Map of extracted entries
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> extractEntries(int maxEntries) {
        synchronized (lock) {
            LinkedHashMap<String, BatchingExecutionContext<RequestT, ResponseT>> extractedEntries = new LinkedHashMap<>();
            String nextEntry;
            while (extractedEntries.size() < maxEntries && hasNextBatchEntry()) {
                nextEntry = nextBatchEntry();
                extractedEntries.put(nextEntry, idToBatchContext.get(nextEntry));
                idToBatchContext.remove(nextEntry);
            }
            return extractedEntries;
        }
    }

    private boolean hasNextBatchEntry() {
        return idToBatchContext.containsKey(Integer.toString(nextBatchEntry));
    }

    private String nextBatchEntry() {
        if (nextBatchEntry == Integer.MAX_VALUE) {
            nextBatchEntry = 0;
        }
        return Integer.toString(nextBatchEntry++);
    }

    /**
     * Updates the scheduled flush task for this buffer.
     *
     * @param scheduledFlush The new scheduled flush task
     */
    public void putScheduledFlush(ScheduledFuture<?> scheduledFlush) {
        this.scheduledFlush = scheduledFlush;
    }

    /**
     * Cancels the scheduled flush task for this buffer.
     */
    public void cancelScheduledFlush() {
        if (scheduledFlush != null) {
            scheduledFlush.cancel(false);
        }
    }

    /**
     * Gets all response futures in this buffer.
     *
     * @return Collection of response futures
     */
    public Collection<CompletableFuture<ResponseT>> responses() {
        return idToBatchContext.values()
                               .stream()
                               .map(BatchingExecutionContext::response)
                               .collect(Collectors.toList());
    }

    /**
     * Clears all entries from this buffer.
     */
    public void clear() {
        idToBatchContext.clear();
    }
}
