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


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import software.amazon.awssdk.utils.uri.SdkUri;

@BenchmarkMode( {Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 5, time = 10)
@Threads(25)
public class SdkUriCacheBenchmark {

    public static void main(String[] args) throws IOException {
        Main.main(args);
    }

    @State(Scope.Thread)
    public static class BenchmarkState {
        @Param( {"true", "false"})
        private String useCache;

        @Param( {"normal", "accountId", "both"})
        private String uriType;

        private List<String> accountIdUris;
        private List<String> uris;

        @Setup
        public void setup() {
            this.uris = new ArrayList<>();
            this.accountIdUris = new ArrayList<>();

            // put each uris 3 times so that it hit the cache the 2nd and 3rd time
            for (int i = 0; i < 3; i++) {

                // account id style endpoints
                for (int j = 0; i < 20; i++) {
                    accountIdUris.add("https://123456789012.ddb.us-east-1." + j + ".amazonaws.com");
                }

                // non-cached uri
                for (int j = 0; i < 20; i++) {
                    uris.add("https://ddb.us-east-1." + j + ".amazonaws.com");
                }
            }
        }

        @TearDown
        public void teardown() {
            uris.clear();
            accountIdUris.clear();
        }
    }

    @Benchmark
    public void benchmark(BenchmarkState state, Blackhole blackhole) {
        List<String> toParse = new ArrayList<>();
        switch (state.uriType) {
            case "normal": {
                toParse.addAll(state.uris);
                break;
            }
            case "accountId": {
                toParse.addAll(state.accountIdUris);
                break;
            }
            case "both" : {
                toParse.addAll(state.uris);
                toParse.addAll(state.accountIdUris);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + state.uriType);
        }

        if (Boolean.parseBoolean(state.useCache)) {
            toParse.parallelStream().forEach(uri -> blackhole.consume(SdkUri.getInstance().create(uri)));
        } else {
            toParse.parallelStream().forEach(uri -> blackhole.consume(URI.create(uri)));
        }
    }

}
