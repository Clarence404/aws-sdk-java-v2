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

package software.amazon.awssdk.core.internal.async;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.NonRetryableException;
import software.amazon.awssdk.core.internal.util.NoopSubscription;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * An implementation of {@link AsyncRequestBody} that buffers the entire content and can be subscribed multiple times.
 */
@SdkInternalApi
public final class BufferingAsyncRequestBody implements AsyncRequestBody, SdkAutoCloseable {
    private static final Logger log = Logger.loggerFor(BufferingAsyncRequestBody.class);

    private final Long length;
    private final List<ByteBuffer> bufferedData = new ArrayList<>();
    private final AtomicBoolean dataReady = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Subscriber<? super ByteBuffer> subscriber;
    private final AtomicLong outstandingDemand = new AtomicLong();

    BufferingAsyncRequestBody(Long length) {
        this.length = length;
    }

    public void send(ByteBuffer data) {
        synchronized (bufferedData) {
            bufferedData.add(data);
        }
    }

    public void complete() {
        dataReady.set(true);
    }

    @Override
    public Optional<Long> contentLength() {
        return Optional.ofNullable(length);
    }

    @Override
    public void subscribe(Subscriber<? super ByteBuffer> subscriber) {
        if (subscriber == null) {
            throw new NullPointerException("Subscription MUST NOT be null.");
        }

        if (!dataReady.get()) {
            // Should never happen
            subscriber.onSubscribe(new NoopSubscription(subscriber));
            subscriber.onError(NonRetryableException.create(
                "Unexpected error occurred. Data is not ready to be subscribed"));
            return;
        }

        if (closed.get()) {
            subscriber.onSubscribe(new NoopSubscription(subscriber));
            subscriber.onError(NonRetryableException.create(
                "AsyncRequestBody has been closed"));
            return;
        }

        this.subscriber = subscriber;

        try {
            ReplayableByteBufferSubscription replayableByteBufferSubscription =
                new ReplayableByteBufferSubscription(bufferedData);
            subscriber.onSubscribe(replayableByteBufferSubscription);
        } catch (Throwable ex) {
            log.error(() -> "subscriber violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.", ex);
        }
    }

    @Override
    public String body() {
        return BodyType.BYTES.getName();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {

            synchronized (bufferedData) {
                bufferedData.clear();
            }

            if (subscriber != null) {
                subscriber.onError(NonRetryableException.create(
                    "AsyncRequestBody has been closed"));
            }

        }
    }

    private class ReplayableByteBufferSubscription implements Subscription {
        private final AtomicInteger index = new AtomicInteger(0);
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final List<ByteBuffer> buffers;
        private final AtomicBoolean processingRequest = new AtomicBoolean(false);

        public ReplayableByteBufferSubscription(List<ByteBuffer> buffers) {
            this.buffers = buffers;
        }

        @Override
        public void request(long n) {
            if (n <= 0) {
                subscriber.onError(new IllegalArgumentException("§3.9: non-positive requests are not allowed!"));
                subscriber = null;
                return;
            }

            if (done.get()) {
                return;
            }

            long newDemand = outstandingDemand.updateAndGet(current -> {
                if (Long.MAX_VALUE - current < n) {
                    return Long.MAX_VALUE;
                }

                return current + n;
            });
            log.trace(() -> "Increased demand to " + newDemand);
            processRequest();
        }

        private void processRequest() {
            do {
                if (!processingRequest.compareAndSet(false, true)) {
                    // Some other thread is processing the queue, so we don't need to.
                    return;
                }

                try {
                    if (done.get()) {
                        return;
                    }

                    int currentIndex = this.index.getAndIncrement();

                    ByteBuffer buffer = buffers.get(currentIndex);
                    subscriber.onNext(buffer.asReadOnlyBuffer());
                    outstandingDemand.decrementAndGet();

                    if (currentIndex == buffers.size() - 1) {
                        subscriber.onComplete();
                        subscriber = null;
                        return;
                    }
                } catch (Throwable e) {
                    //panicAndDie(e);
                    break;
                } finally {
                    processingRequest.set(false);
                }

            } while (outstandingDemand.get() > 0 && index.get() < buffers.size());
        }

        @Override
        public void cancel() {
            done.set(true);
            subscriber = null;
        }
    }
}
