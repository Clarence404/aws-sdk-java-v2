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

package software.amazon.awssdk.benchmark;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.endpoints.AccountIdEndpointMode;
import software.amazon.awssdk.metrics.publishers.cloudwatch.CloudWatchMetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.uri.SdkURI;

/**
 *
 */
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 2, time = 15)
@Measurement(iterations = 5, time = 10)
@Threads(25)
public class AccountIdEndpointCacheBenchmark {
    private static final Logger log = Logger.loggerFor(AccountIdEndpointCacheBenchmark.class);

    private static final String DISABLED_ACCOUNT_ID_CACHE = "CACHE_DISABLED";
    private static final String ENABLED_ACCOUNT_ID_CACHE = "CACHE_ENABLED";

    private static final String ACCOUNT_ID_ENDPOINT_MODE = "ACCOUNT_ENDPOINT";
    private static final String STANDARD_ENDPOINT_MODE = "STANDARD_ENDPOINT";

    @State(Scope.Thread)
    public static class DynamoDbState {
        @Param( {ACCOUNT_ID_ENDPOINT_MODE, STANDARD_ENDPOINT_MODE})
        private String endpointMode;

        @Param( {DISABLED_ACCOUNT_ID_CACHE, ENABLED_ACCOUNT_ID_CACHE})
        private String accountIdCache;

        // private DynamoDbClient stubbedClient;
        private DynamoDbClient liveClient;
        private PutItemRequest request;

        @Setup(Level.Trial)
        public void setup() {

            if (DISABLED_ACCOUNT_ID_CACHE.equals(accountIdCache)) {
                SdkURI.getInstance().setBackdoorDisable(true);
            }

            if (ENABLED_ACCOUNT_ID_CACHE.equals(accountIdCache)) {
                SdkURI.getInstance().setBackdoorDisable(false);
            }

            AccountIdEndpointMode mode = ACCOUNT_ID_ENDPOINT_MODE.equals(endpointMode)
                                         ? AccountIdEndpointMode.REQUIRED
                                         : AccountIdEndpointMode.DISABLED;

            liveClient = DynamoDbClient.builder()
                                       .region(Region.US_EAST_1)
                                       .accountIdEndpointMode(mode)
                                       .overrideConfiguration(c -> c.addMetricPublisher(
                                           CloudWatchMetricPublisher.builder()
                                                                    .namespace("CacheBenchmark/" + endpointMode + "/" + accountIdCache)
                                                                    .build()))
                                       .build();

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
            item.put("data", AttributeValue.builder().s("benchmark-data-" + System.currentTimeMillis()).build());

            request = PutItemRequest.builder()
                                    .tableName("ddb-benchmarking")
                                    .item(item)
                                    .build();
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            liveClient.close();
        }
    }

    @Benchmark
    public void benchmark(DynamoDbState state, Blackhole blackhole) {
        blackhole.consume(state.liveClient.putItem(state.request));
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
