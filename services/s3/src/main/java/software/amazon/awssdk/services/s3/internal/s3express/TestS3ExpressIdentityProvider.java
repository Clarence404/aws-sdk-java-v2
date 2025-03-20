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

package software.amazon.awssdk.services.s3.internal.s3express;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.s3.s3express.S3ExpressSessionCredentials;

public final class TestS3ExpressIdentityProvider implements IdentityProvider<S3ExpressSessionCredentials> {
    private final S3ExpressSessionCredentials creds;
    public TestS3ExpressIdentityProvider(S3ExpressSessionCredentials creds) {
        this.creds = creds;
    }

    @Override
    public Class<S3ExpressSessionCredentials> identityType() {
        return S3ExpressSessionCredentials.class;
    }

    @Override
    public CompletableFuture<S3ExpressSessionCredentials> resolveIdentity(ResolveIdentityRequest request) {
        return CompletableFuture.completedFuture(this.creds);
    }
}
