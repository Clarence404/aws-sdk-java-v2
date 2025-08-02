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

package software.amazon.awssdk.core.async;

import java.util.Objects;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.Validate;
import software.amazon.awssdk.utils.builder.CopyableBuilder;
import software.amazon.awssdk.utils.builder.ToCopyableBuilder;

/**
 * A wrapper for {@link AsyncRequestBody} that provides additional functionality such as cleanup operations.
 */
@SdkPublicApi
public final class AsyncRequestBodyWrapper implements SdkAutoCloseable,
    ToCopyableBuilder<AsyncRequestBodyWrapper.Builder, AsyncRequestBodyWrapper> {
    
    private final AsyncRequestBody requestBody;
    private final Runnable onClose;

    private AsyncRequestBodyWrapper(DefaultBuilder builder) {
        this.requestBody = Validate.paramNotNull(builder.requestBody, "requestBody");
        this.onClose = builder.onClose;
    }

    /**
     * @return the wrapped {@link AsyncRequestBody}.
     */
    public AsyncRequestBody requestBody() {
        return requestBody;
    }

    /**
     * @return the cleanup operation to run when this wrapper is closed.
     */
    public Runnable onClose() {
        return onClose;
    }

    @Override
    public void close() {
        if (onClose != null) {
            onClose.run();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AsyncRequestBodyWrapper that = (AsyncRequestBodyWrapper) o;

        if (!Objects.equals(requestBody, that.requestBody)) {
            return false;
        }
        return Objects.equals(onClose, that.onClose);
    }

    @Override
    public int hashCode() {
        int result = requestBody != null ? requestBody.hashCode() : 0;
        result = 31 * result + (onClose != null ? onClose.hashCode() : 0);
        return result;
    }

    /**
     * Create a {@link Builder}, used to create a {@link AsyncRequestBodyWrapper}.
     */
    public static Builder builder() {
        return new DefaultBuilder();
    }

    @Override
    public AsyncRequestBodyWrapper.Builder toBuilder() {
        return new DefaultBuilder(this);
    }

    /**
     * Builder for {@link AsyncRequestBodyWrapper}.
     */
    public interface Builder extends CopyableBuilder<AsyncRequestBodyWrapper.Builder, AsyncRequestBodyWrapper> {

        /**
         * Configures the {@link AsyncRequestBody} to be wrapped.
         *
         * @param requestBody the request body to wrap
         * @return This object for method chaining.
         */
        Builder requestBody(AsyncRequestBody requestBody);

        /**
         * Configures the cleanup operation to run when the wrapper is closed.
         *
         * @param onClose the cleanup operation
         * @return This object for method chaining.
         */
        Builder onClose(Runnable onClose);
    }

    private static final class DefaultBuilder implements Builder {
        private AsyncRequestBody requestBody;
        private Runnable onClose;

        private DefaultBuilder() {
        }

        private DefaultBuilder(AsyncRequestBodyWrapper asyncRequestBodyWrapper) {
            this.requestBody = asyncRequestBodyWrapper.requestBody;
            this.onClose = asyncRequestBodyWrapper.onClose;
        }

        @Override
        public Builder requestBody(AsyncRequestBody requestBody) {
            this.requestBody = requestBody;
            return this;
        }

        @Override
        public Builder onClose(Runnable onClose) {
            this.onClose = onClose;
            return this;
        }

        @Override
        public AsyncRequestBodyWrapper build() {
            return new AsyncRequestBodyWrapper(this);
        }
    }
}
