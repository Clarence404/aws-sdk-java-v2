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

package software.amazon.awssdk.services.sqs.batchmanager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import software.amazon.awssdk.services.sqs.internal.batchmanager.BatchRequestExtractor;
import software.amazon.awssdk.services.sqs.internal.batchmanager.FlushPolicy;
import software.amazon.awssdk.services.sqs.internal.batchmanager.RequestBatchBuffer;
import software.amazon.awssdk.services.sqs.internal.batchmanager.BatchingExecutionContext;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.awssdk.services.sqs.internal.batchmanager.ResponseBatchConfiguration.MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES;

class RequestBatchBufferTest {

    private RequestBatchBuffer<String, String> batchBuffer;
    private BatchRequestExtractor<String, String> requestExtractor;
    private FlushPolicy<String, String> flushPolicy;
    private ScheduledFuture<?> scheduledFlush;

    private static final int maxBufferSize = 1000;
    private static final int maxBatchItems = 10;

    @BeforeEach
    void setUp() {
        scheduledFlush = mock(ScheduledFuture.class);
        flushPolicy = new FlushPolicy<>(maxBatchItems, MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES);
        requestExtractor = new BatchRequestExtractor<>(flushPolicy);
        batchBuffer = new RequestBatchBuffer<>(scheduledFlush, maxBufferSize);
    }

    @Test
    void whenPutRequestThenBufferContainsRequest() {
        CompletableFuture<String> response = new CompletableFuture<>();
        batchBuffer.put("request1", response);
        assertEquals(1, batchBuffer.responses().size());
    }

    @Test
    void whenFlushableRequestsThenReturnRequestsUpToMaxBatchItems() {
        // Create a flush policy that will flush when there's at least 1 item
        FlushPolicy<String, String> singleItemFlushPolicy = new FlushPolicy<>(1, MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES);
        BatchRequestExtractor<String, String> singleItemExtractor = new BatchRequestExtractor<>(singleItemFlushPolicy);

        CompletableFuture<String> response = new CompletableFuture<>();
        batchBuffer.put("request1", response);

        // Using the BatchRequestExtractor with a policy that flushes at 1 item
        Map<String, BatchingExecutionContext<String, String>> flushedRequests =
            singleItemExtractor.extractFlushableRequests(batchBuffer);

        assertEquals(1, flushedRequests.size());
        assertTrue(flushedRequests.containsKey("0"));
    }


    @Test
    void whenFlushableScheduledRequestsThenReturnAllRequests() {
        CompletableFuture<String> response = new CompletableFuture<>();
        batchBuffer.put("request1", response);

        // Using the BatchRequestExtractor to get scheduled flushable requests
        Map<String, BatchingExecutionContext<String, String>> flushedRequests =
            requestExtractor.extractScheduledFlushableRequests(batchBuffer, 1);

        assertEquals(1, flushedRequests.size());
        assertTrue(flushedRequests.containsKey("0"));
    }


    @Test
    void whenMaxBufferSizeReachedThenThrowException() {
        // Create a buffer with a small max size
        RequestBatchBuffer<String, String> smallBuffer = new RequestBatchBuffer<>(scheduledFlush, 10);

        // Fill the buffer to capacity
        for (int i = 0; i < 10; i++) {
            smallBuffer.put("request" + i, new CompletableFuture<>());
        }

        // Try to add one more request, which should throw an exception
        assertThrows(IllegalStateException.class, () ->
            smallBuffer.put("request11", new CompletableFuture<>()));
    }

    @Test
    void whenCancelScheduledFlushThenFlushIsCancelled() {
        batchBuffer.cancelScheduledFlush();
        verify(scheduledFlush).cancel(false);
    }


    @Test
    void whenGetResponsesThenReturnAllResponses() {
        CompletableFuture<String> response1 = new CompletableFuture<>();
        CompletableFuture<String> response2 = new CompletableFuture<>();
        batchBuffer.put("request1", response1);
        batchBuffer.put("request2", response2);
        Collection<CompletableFuture<String>> responses = batchBuffer.responses();
        assertEquals(2, responses.size());
        assertTrue(responses.contains(response1));
        assertTrue(responses.contains(response2));
    }

    @Test
    void whenClearBufferThenBufferIsEmpty() {
        CompletableFuture<String> response = new CompletableFuture<>();
        batchBuffer.put("request1", response);
        batchBuffer.clear();
        assertTrue(batchBuffer.isEmpty());
    }


    @Test
    void whenExtractFlushedEntriesThenReturnCorrectEntries() {
        for (int i = 0; i < 5; i++) {
            batchBuffer.put("request" + i, new CompletableFuture<>());
        }

        // Extract entries directly from the buffer
        Map<String, BatchingExecutionContext<String, String>> flushedEntries = batchBuffer.extractEntries(5);
        assertEquals(5, flushedEntries.size());
    }


    @Test
    void whenHasNextBatchEntryThenReturnTrue() {
        batchBuffer.put("request1", new CompletableFuture<>());

        // Extract entries and check if the expected key exists
        Map<String, BatchingExecutionContext<String, String>> entries = batchBuffer.extractEntries(1);
        assertTrue(entries.containsKey("0"));
    }


    @Test
    void whenNextBatchEntryThenReturnNextEntryId() {
        batchBuffer.put("request1", new CompletableFuture<>());

        // Extract entries and check the key
        Map<String, BatchingExecutionContext<String, String>> entries = batchBuffer.extractEntries(1);
        assertEquals("0", entries.keySet().iterator().next());
    }

    @Test
    void whenRequestPassedWithLessBytesinArgs_thenCheckForSizeOnly_andDonotFlush() {
        RequestBatchBuffer<SendMessageRequest, SendMessageResponse> messageBatchBuffer =
            new RequestBatchBuffer<>(scheduledFlush, maxBufferSize);
        FlushPolicy<SendMessageRequest, SendMessageResponse> messageFlushPolicy =
            new FlushPolicy<>(5, MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES);
        BatchRequestExtractor<SendMessageRequest, SendMessageResponse> messageRequestExtractor =
            new BatchRequestExtractor<>(messageFlushPolicy);

        // Add 5 messages to the buffer
        for (int i = 0; i < 5; i++) {
            messageBatchBuffer.put(SendMessageRequest.builder().build(), new CompletableFuture<>());
        }

        // Check if we should flush before adding a small message
        Map<String, BatchingExecutionContext<SendMessageRequest, SendMessageResponse>> flushedEntries =
            messageRequestExtractor.extractFlushableRequestsBeforeAdd(
                messageBatchBuffer, SendMessageRequest.builder().messageBody("Hi").build());

        assertEquals(0, flushedEntries.size());
    }

    @Test
    void testFlushWhenPayloadExceedsMaxSize() {
        RequestBatchBuffer<SendMessageRequest, SendMessageResponse> messageBatchBuffer =
            new RequestBatchBuffer<>(scheduledFlush, maxBufferSize);
        FlushPolicy<SendMessageRequest, SendMessageResponse> messageFlushPolicy =
            new FlushPolicy<>(5, MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES);
        BatchRequestExtractor<SendMessageRequest, SendMessageResponse> messageRequestExtractor =
            new BatchRequestExtractor<>(messageFlushPolicy);

        // Create a large message that would exceed the max payload size
        String largeMessageBody = createLargeString('a', 245_760);
        messageBatchBuffer.put(
            SendMessageRequest.builder().messageBody(largeMessageBody).build(),
            new CompletableFuture<>());

        // Check if we should flush before adding another message
        Map<String, BatchingExecutionContext<SendMessageRequest, SendMessageResponse>> flushedEntries =
            messageRequestExtractor.extractFlushableRequestsBeforeAdd(
                messageBatchBuffer, SendMessageRequest.builder().messageBody("NewMessage").build());

        assertEquals(1, flushedEntries.size());
    }

    @Test
    void testFlushWhenCumulativePayloadExceedsMaxSize() {
        RequestBatchBuffer<SendMessageRequest, SendMessageResponse> messageBatchBuffer =
            new RequestBatchBuffer<>(scheduledFlush, maxBufferSize);
        FlushPolicy<SendMessageRequest, SendMessageResponse> messageFlushPolicy =
            new FlushPolicy<>(5, MAX_SEND_MESSAGE_PAYLOAD_SIZE_BYTES);
        BatchRequestExtractor<SendMessageRequest, SendMessageResponse> messageRequestExtractor =
            new BatchRequestExtractor<>(messageFlushPolicy);

        // Add two large messages that together exceed the max payload size
        String largeMessageBody = createLargeString('a', 130_000);
        messageBatchBuffer.put(
            SendMessageRequest.builder().messageBody(largeMessageBody).build(),
            new CompletableFuture<>());
        messageBatchBuffer.put(
            SendMessageRequest.builder().messageBody(largeMessageBody).build(),
            new CompletableFuture<>());

        // Check if we should flush before adding another message
        Map<String, BatchingExecutionContext<SendMessageRequest, SendMessageResponse>> flushedEntries =
            messageRequestExtractor.extractFlushableRequestsBeforeAdd(
                messageBatchBuffer, SendMessageRequest.builder().messageBody("NewMessage").build());

        // Flushes both the messages since their sum is greater than 256KB
        assertEquals(2, flushedEntries.size());
    }


    private String createLargeString(char ch, int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(ch);
        }
        return sb.toString();
    }

}