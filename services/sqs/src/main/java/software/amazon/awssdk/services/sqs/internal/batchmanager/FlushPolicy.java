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

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Policy that determines when requests should be flushed from a batch buffer.
 *
 * @param <RequestT> The type of request being batched
 * @param <ResponseT> The type of response for each request
 */
@SdkInternalApi
public final class FlushPolicy<RequestT, ResponseT> {
    private final int maxBatchItems;
    private final int maxBatchBytesSize;

    /**
     * Creates a new flush policy.
     *
     * @param maxBatchItems Maximum number of items in a batch
     * @param maxBatchBytesSize Maximum size in bytes of a batch, or 0 if no limit
     */
    public FlushPolicy(int maxBatchItems, int maxBatchBytesSize) {
        this.maxBatchItems = maxBatchItems;
        this.maxBatchBytesSize = maxBatchBytesSize;
    }

    /**
     * Determines if a buffer should be flushed based on its current state.
     *
     * @param buffer The buffer to check
     * @return True if the buffer should be flushed
     */
    public boolean shouldFlush(RequestBatchBuffer<RequestT, ResponseT> buffer) {
        return buffer.size() >= maxBatchItems ||
               (maxBatchBytesSize > 0 && buffer.getCurrentBatchSizeInBytes() > maxBatchBytesSize);
    }

    /**
     * Determines if adding a new request would cause the buffer to exceed byte size limits.
     *
     * @param buffer The buffer to check
     * @param request The request that would be added
     * @return True if the buffer should be flushed before adding the request
     */
    public boolean shouldFlushBeforeAdd(RequestBatchBuffer<RequestT, ResponseT> buffer, RequestT request) {
        if (maxBatchBytesSize <= 0 || buffer.isEmpty()) {
            return false;
        }

        int incomingRequestBytes = RequestPayloadCalculator.calculateMessageSize(request).orElse(0);
        return buffer.getCurrentBatchSizeInBytes() + incomingRequestBytes > maxBatchBytesSize;
    }

    /**
     * Determines how many entries should be flushed.
     *
     * @param buffer The buffer containing entries to flush
     * @return The number of entries that should be flushed
     */
    public int getFlushableEntryCount(RequestBatchBuffer<RequestT, ResponseT> buffer) {
        return Math.min(buffer.size(), maxBatchItems);
    }
}
