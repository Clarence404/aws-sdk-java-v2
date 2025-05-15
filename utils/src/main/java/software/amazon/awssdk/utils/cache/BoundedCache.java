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

package software.amazon.awssdk.utils.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.Validate;

public final class BoundedCache<K, V>  {

    private static final Logger log = Logger.loggerFor(BoundedCache.class);

    private static final int DEFAULT_SIZE = 100;

    private final ConcurrentHashMap<K, V> cache;
    private final Function<K, V> valueSupplier;
    private final int maxCacheSize;
    private final Object cacheLock;

    private BoundedCache(Builder<K, V> b) {
        this.valueSupplier = b.supplier;
        Integer customSize = b.maxSize == null ? null : Validate.isPositive(b.maxSize, "size");
        this.maxCacheSize = customSize != null ? customSize : DEFAULT_SIZE;
        this.cache = new ConcurrentHashMap<>();
        this.cacheLock = new Object();
    }

    /**
     * Get a value based on the key. If the value exists in the cache, it's returned.
     * Otherwise, the value is calculated based on the supplied function {@link Builder#builder(Function)}.
     */
    public V get(K key) {
        V value = cache.get(key);
        if (value != null) {
            return value;
        }

        V newValue = valueSupplier.apply(key);

        // If the value is null, just return it without caching
        if (newValue == null) {
            return null;
        }

        synchronized (cacheLock) {
            // Check again inside the synchronized block in case another thread added the value
            value = cache.get(key);
            if (value != null) {
                return value;
            }

            if (cache.size() >= maxCacheSize) {
                cleanup();
            }

            cache.put(key, newValue);
            return newValue;
        }
    }

    /**
     * Clean up the cache by removing a random entry
     */
    private void cleanup() {
        Iterator<K> iterator = cache.keySet().iterator();
        if (iterator.hasNext()) {
            K key = iterator.next();
            cache.remove(key);
        }
    }

    public int size() {
        return cache.size();
    }

    public static <K, V> BoundedCache.Builder<K, V> builder(Function<K, V> supplier) {
        return new Builder<>(supplier);
    }

    public boolean containsKey(K key) {
        return cache.containsKey(key);
    }

    public static final class Builder<K, V> {

        private final Function<K, V> supplier;
        private Integer maxSize;

        private Builder(Function<K, V> supplier) {
            this.supplier = supplier;
        }

        public Builder<K, V> maxSize(Integer maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public BoundedCache<K, V> build() {
            return new BoundedCache<>(this);
        }
    }
}
