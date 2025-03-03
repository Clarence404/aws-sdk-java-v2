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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.utils.Either;
import software.amazon.awssdk.utils.Validate;


/**
 * Abstract base class for managing batched requests.
 *
 * @param <RequestT> The type of request being batched
 * @param <ResponseT> The type of response for each request
 * @param <BatchResponseT> The type of response for the batch operation
 */
@SdkInternalApi
public abstract class RequestBatchManager<RequestT, ResponseT, BatchResponseT> {

    // abm stands for Automatic Batching Manager
    public static final Consumer<AwsRequestOverrideConfiguration.Builder> USER_AGENT_APPLIER =
        b -> b.addApiName(ApiName.builder().version("abm").name("hll").build());

    protected final RequestBatchConfiguration batchConfiguration;

    private final int maxBatchItems;
    private final Duration sendRequestFrequency;
    private final BatchingMap <RequestT, ResponseT> BatchingMap ;
    private final ScheduledExecutorService scheduledExecutor;
    private final Set<CompletableFuture<BatchResponseT>> pendingBatchResponses;
    private final Set<CompletableFuture<ResponseT>> pendingResponses;


    protected RequestBatchManager(RequestBatchConfiguration overrideConfiguration,
                                  ScheduledExecutorService scheduledExecutor) {
        batchConfiguration = overrideConfiguration;
        this.maxBatchItems = batchConfiguration.maxBatchItems();
        this.sendRequestFrequency = batchConfiguration.sendRequestFrequency();
        this.scheduledExecutor = Validate.notNull(scheduledExecutor, "Null scheduledExecutor");
        pendingBatchResponses = ConcurrentHashMap.newKeySet();
        pendingResponses = ConcurrentHashMap.newKeySet();
        this.BatchingMap  = new BatchingMap <>(overrideConfiguration);

    }

    public CompletableFuture<ResponseT> batchRequest(RequestT request) {
        CompletableFuture<ResponseT> response = new CompletableFuture<>();
        pendingResponses.add(response);
        response.whenComplete((r, t) -> pendingResponses.remove(response));

        try {
            String batchKey = getBatchKey(request);
            // Handle potential byte size overflow only if there are requests in map and if feature enabled
            if (BatchingMap .containsKey(batchKey) && batchConfiguration.maxBatchBytesSize() > 0) {
                Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush =
                    BatchingMap .getRequestsToFlushBeforeAdd(batchKey, request);

                if (!requestsToFlush.isEmpty()) {
                    manualFlushBuffer(batchKey, requestsToFlush);
                }
            }

            // Add request and response to the manager, scheduling a flush if necessary
            BatchingMap .addRequest(batchKey,
                                          () -> scheduleBufferFlush(batchKey,
                                                                    sendRequestFrequency.toMillis(),
                                                                    scheduledExecutor),
                                          request,
                                          response);

            // Immediately flush if the batch is ready
            Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush =
                BatchingMap .getRequestsToFlush(batchKey);

            if (!requestsToFlush.isEmpty()) {
                manualFlushBuffer(batchKey, requestsToFlush);
            }

        } catch (Exception e) {
            response.completeExceptionally(e);
        }

        return response;
    }

    protected abstract CompletableFuture<BatchResponseT> batchAndSend(List<IdentifiableMessage<RequestT>> identifiedRequests,
                                                                      String batchKey);

    protected abstract String getBatchKey(RequestT request);

    protected abstract List<Either<IdentifiableMessage<ResponseT>,
        IdentifiableMessage<Throwable>>> mapBatchResponse(BatchResponseT batchResponse);

    private void manualFlushBuffer(String batchKey,
                                   Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush) {
        BatchingMap .cancelScheduledFlush(batchKey);
        flushBuffer(batchKey, requestsToFlush);
        BatchingMap .updateScheduledFlush(batchKey,
                                                scheduleBufferFlush(batchKey,
                                                                    sendRequestFrequency.toMillis(),
                                                                    scheduledExecutor));
    }

    private void flushBuffer(String batchKey, Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush) {
        List<IdentifiableMessage<RequestT>> requestEntries = new ArrayList<>();
        requestsToFlush.forEach((contextId, batchExecutionContext) ->
                                    requestEntries.add(new IdentifiableMessage<>(contextId, batchExecutionContext.request())));
        if (!requestEntries.isEmpty()) {
            CompletableFuture<BatchResponseT> pendingBatchingRequest = batchAndSend(requestEntries, batchKey);
            pendingBatchResponses.add(pendingBatchingRequest);

            pendingBatchingRequest.whenComplete((result, ex) -> {
                handleAndCompleteResponses(result, ex, requestsToFlush);
                pendingBatchResponses.remove(pendingBatchingRequest);
            });
        }
    }

    private void handleAndCompleteResponses(BatchResponseT batchResult, Throwable exception,
                                            Map<String, BatchingExecutionContext<RequestT, ResponseT>> requests) {
        requests.forEach((contextId, batchExecutionContext) ->  pendingResponses.add(batchExecutionContext.response()));
        if (exception != null) {
            requests.forEach((contextId, batchExecutionContext) -> batchExecutionContext.response()
                                                                                        .completeExceptionally(exception));
        } else {
            mapBatchResponse(batchResult)
                .forEach(
                    response -> response.map(actualResponse -> requests.get(actualResponse.id())
                                                                       .response()
                                                                       .complete(actualResponse.message()),
                                             throwable -> requests.get(throwable.id())
                                                                  .response()
                                                                  .completeExceptionally(throwable.message())));
        }
        requests.clear();
    }

    private ScheduledFuture<?> scheduleBufferFlush(String batchKey, long timeOutInMs,
                                                   ScheduledExecutorService scheduledExecutor) {
        return scheduledExecutor.scheduleAtFixedRate(() -> performScheduledFlush(batchKey), timeOutInMs, timeOutInMs,
                                                     TimeUnit.MILLISECONDS);
    }

    private void performScheduledFlush(String batchKey) {
        Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush =
            BatchingMap .getScheduledRequestsToFlush(batchKey, maxBatchItems);
        if (!requestsToFlush.isEmpty()) {
            flushBuffer(batchKey, requestsToFlush);
        }
    }

    public void close() {
        BatchingMap .forEach((batchKey, buffer) -> {
            BatchingMap .cancelScheduledFlush(batchKey);
            Map<String, BatchingExecutionContext<RequestT, ResponseT>> requestsToFlush =
                BatchingMap .getRequestsToFlush(batchKey);

            while (!requestsToFlush.isEmpty()) {
                flushBuffer(batchKey, requestsToFlush);
                requestsToFlush = BatchingMap .getRequestsToFlush(batchKey);
            }
        });
        pendingBatchResponses.forEach(future -> future.cancel(true));
        pendingResponses.forEach(future -> future.cancel(true));
        BatchingMap .clear();
    }
}
