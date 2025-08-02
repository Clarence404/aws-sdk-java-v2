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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

public class AsyncRequestBodyWrapperTest {

    @Test
    void equalsHashcode() {
        EqualsVerifier.forClass(AsyncRequestBodyWrapper.class)
                      .verify();
    }

    @Test
    void builder_withAllFields_shouldBuildSuccessfully() {
        AsyncRequestBody requestBody = mock(AsyncRequestBody.class);
        Runnable onClose = mock(Runnable.class);
        
        AsyncRequestBodyWrapper wrapper = AsyncRequestBodyWrapper.builder()
                                                                 .requestBody(requestBody)
                                                                 .onClose(onClose)
                                                                 .build();
        
        assertThat(wrapper.requestBody()).isEqualTo(requestBody);
        assertThat(wrapper.onClose()).isEqualTo(onClose);
    }

    @Test
    void toBuilder_shouldCopyProperties() {
        AsyncRequestBody requestBody = mock(AsyncRequestBody.class);
        Runnable onClose = mock(Runnable.class);
        
        AsyncRequestBodyWrapper original = AsyncRequestBodyWrapper.builder()
                                                                  .requestBody(requestBody)
                                                                  .onClose(onClose)
                                                                  .build();
        
        AsyncRequestBodyWrapper copy = original.toBuilder().build();
        
        assertThat(copy).isEqualTo(original);
        assertThat(copy.requestBody()).isEqualTo(original.requestBody());
        assertThat(copy.onClose()).isEqualTo(original.onClose());
    }

    @Test
    void toBuilder_withModifications_shouldCreateModifiedCopy() {
        AsyncRequestBody originalRequestBody = mock(AsyncRequestBody.class);
        AsyncRequestBody newRequestBody = mock(AsyncRequestBody.class);
        Runnable originalOnClose = mock(Runnable.class);
        Runnable newOnClose = mock(Runnable.class);
        
        AsyncRequestBodyWrapper original = AsyncRequestBodyWrapper.builder()
                                                                  .requestBody(originalRequestBody)
                                                                  .onClose(originalOnClose)
                                                                  .build();
        
        AsyncRequestBodyWrapper modified = original.toBuilder()
                                                   .requestBody(newRequestBody)
                                                   .onClose(newOnClose)
                                                   .build();
        
        assertThat(modified.requestBody()).isEqualTo(newRequestBody);
        assertThat(modified.onClose()).isEqualTo(newOnClose);
        assertThat(modified).isNotEqualTo(original);
    }

    @Test
    void requestBodyIsNull_shouldThrowException() {
        assertThatThrownBy(() ->
                               AsyncRequestBodyWrapper.builder()
                                                      .build())
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("requestBody");
    }

    @Test
    void close_withOnClose_shouldExecuteOnClose() {
        AsyncRequestBody requestBody = mock(AsyncRequestBody.class);
        Runnable onClose = mock(Runnable.class);
        
        AsyncRequestBodyWrapper wrapper = AsyncRequestBodyWrapper.builder()
                                                                 .requestBody(requestBody)
                                                                 .onClose(onClose)
                                                                 .build();
        
        wrapper.close();
        
        verify(onClose, times(1)).run();
    }

    @Test
    void close_withoutOnClose_shouldNotThrowException() {
        AsyncRequestBody requestBody = mock(AsyncRequestBody.class);
        
        AsyncRequestBodyWrapper wrapper = AsyncRequestBodyWrapper.builder()
                                                                 .requestBody(requestBody)
                                                                 .build();
        
        // Should not throw any exception
        wrapper.close();
    }
}
