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
import software.amazon.awssdk.crt.auth.credentials.Credentials;
import software.amazon.awssdk.crt.s3.S3Client;
import software.amazon.awssdk.crt.s3.S3ExpressCredentialsProperties;
import software.amazon.awssdk.crt.s3.S3ExpressCredentialsProvider;
import software.amazon.awssdk.crt.s3.S3ExpressCredentialsProviderFactory;
import software.amazon.awssdk.crt.s3.S3ExpressCredentialsProviderHandler;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.services.s3.s3express.S3ExpressSessionCredentials;

public class S3ExpressCredentialsProviderAdapter implements S3ExpressCredentialsProviderFactory {
    private final IdentityProvider<S3ExpressSessionCredentials> s3ExpressCredentialsProvider;

    public S3ExpressCredentialsProviderAdapter(
        IdentityProvider<S3ExpressSessionCredentials>  sdkProvider) {
        this.s3ExpressCredentialsProvider = sdkProvider;
    }


    @Override
    public S3ExpressCredentialsProvider createS3ExpressCredentialsProvider(S3Client s3Client) {
        S3ExpressCredentialsProviderHandler handler = new S3ExpressCredentialsProviderHandler() {
            @Override
            public CompletableFuture<Credentials> getS3ExpressCredentials(S3ExpressCredentialsProperties s3ExpressCredentialsProperties, Credentials credentials) {
                return s3ExpressCredentialsProvider.resolveIdentity()
                                                   .thenApply(awsCredentials -> new Credentials(awsCredentials.accessKeyId().getBytes(),
                                                                                                awsCredentials.secretAccessKey().getBytes(),
                                                                                                awsCredentials.sessionToken().getBytes()));

            }

            @Override
            public CompletableFuture<Void> destroyProvider() {

                if (s3ExpressCredentialsProvider instanceof AutoCloseable) {
                    try {
                        ((AutoCloseable) s3ExpressCredentialsProvider).close();
                        return CompletableFuture.completedFuture(null);
                    } catch (Exception e) {
                        return null;
                    }
                }
                return CompletableFuture.completedFuture(null);
            }
        };

        return new S3ExpressCredentialsProvider(handler);
    }
}
