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

import software.amazon.awssdk.annotations.SdkInternalApi;
import java.util.Collections;
import java.util.Map;

/**
 * Extracts requests from batch buffers that should be flushed based on flush policies.
 *
 * @param <RequestT> The type of request being batched
 * @param <ResponseT> The type of response for each request
 */
@SdkInternalApi
public final class BatchRequestExtractor<RequestT, ResponseT> {
    private final FlushPolicy<RequestT, ResponseT> flushPolicy;

    /**
     * Creates a new batch request extractor.
     *
     * @param flushPolicy The policy that determines when to flush requests
     */
    public BatchRequestExtractor(FlushPolicy<RequestT, ResponseT> flushPolicy) {
        this.flushPolicy = flushPolicy;
    }

    /**
     * Extracts requests that should be flushed based on current buffer state.
     *
     * @param buffer The buffer containing requests
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> extractFlushableRequests(
        RequestBatchBuffer<RequestT, ResponseT> buffer) {
        if (buffer == null || buffer.isEmpty()) {
            return Collections.emptyMap();
        }

        if (flushPolicy.shouldFlush(buffer)) {
            int entryCount = flushPolicy.getFlushableEntryCount(buffer);
            return buffer.extractEntries(entryCount);
        }
        return Collections.emptyMap();
    }

    /**
     * Extracts requests that should be flushed before adding a new request.
     *
     * @param buffer The buffer containing requests
     * @param request The new request that would be added
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> extractFlushableRequestsBeforeAdd(
        RequestBatchBuffer<RequestT, ResponseT> buffer, RequestT request) {
        if (buffer == null) {
            return Collections.emptyMap();
        }

        if (flushPolicy.shouldFlushBeforeAdd(buffer, request)) {
            int entryCount = flushPolicy.getFlushableEntryCount(buffer);
            return buffer.extractEntries(entryCount);
        }
        return Collections.emptyMap();
    }

    /**
     * Extracts requests during a scheduled flush.
     *
     * @param buffer The buffer containing requests
     * @param maxBatchItems Maximum number of items to extract
     * @return Map of request entries that should be flushed, or empty map if none
     */
    public Map<String, BatchingExecutionContext<RequestT, ResponseT>> extractScheduledFlushableRequests(
        RequestBatchBuffer<RequestT, ResponseT> buffer, int maxBatchItems) {
        if (buffer == null || buffer.isEmpty()) {
            return Collections.emptyMap();
        }
        int entryCount = Math.min(maxBatchItems, flushPolicy.getFlushableEntryCount(buffer));
        return buffer.extractEntries(entryCount);
    }
}
