/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.log.remote.metadata.storage;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;

public class RocksDBMetadataStore implements MetadataStore {
    private static String DB_DIR = "remote-segment-metadata";

    public static class Config extends AbstractConfig {
        public static final String REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP = "remote.log.metadata.rocksdb.cache.size.mb";
        public static final String REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_DOC = "Remote segment metadata store RocksDB in-memory cache size in MB";
        private static final ConfigDef CONFIG_DEF;

        static {
            CONFIG_DEF = new ConfigDef().define(REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP, INT, 50, atLeast(1), MEDIUM, REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_DOC);
        }

        public Config(Map<?, ?> originals) {
            super(CONFIG_DEF, originals);
        }
    }

    public static void loadLibrary() {
        RocksDB.loadLibrary();
    }

    RocksDB db;
    Cache cache;
    RLSMSerDe.RLSMSerializer serializer = new RLSMSerDe.RLSMSerializer();
    RLSMSerDe.RLSMDeserializer deserializer = new RLSMSerDe.RLSMDeserializer();
    private Map<TopicPartition, NavigableMap<Long, RemoteLogSegmentId>> partitionsWithSegmentIds =
        new ConcurrentHashMap<>();

    RocksDBMetadataStore(String logDir, Map<String, ?> configs) {
        Config conf = new Config(configs);

        cache = new LRUCache(conf.getInt(Config.REMOTE_METADATA_ROCKSDB_CACHE_SIZE_MB_PROP));
        final Options options = new Options();
        options.setCreateIfMissing(true);
        options.setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
        File dir = new File(logDir + File.separator + DB_DIR);
        try {
            Files.createDirectories(dir.getAbsoluteFile().toPath());
            db = RocksDB.open(options, dir.getAbsolutePath());
        } catch(IOException | RocksDBException ex) {
            throw new KafkaException("Failed to initialize RocksDB: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void flush() throws IOException {
        FlushOptions options = new FlushOptions();
        options.setWaitForFlush(true);
        try {
            db.flush(options);
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to persist remote segment metadata to local storage: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void load() throws IOException {
        try(RocksIterator itr = db.newIterator()) {
            for (itr.seekToFirst(); itr.isValid(); itr.next()) {
                RemoteLogSegmentMetadata metadata = deserializer.deserialize(null, itr.value());
                partitionsWithSegmentIds.computeIfAbsent(metadata.remoteLogSegmentId().topicPartition(),
                    k -> new ConcurrentSkipListMap<>()).put(metadata.startOffset(), metadata.remoteLogSegmentId());
            }
        }
    }

    @Override
    public void update(TopicPartition tp, RemoteLogSegmentMetadata metadata) {
        try {
            final NavigableMap<Long, RemoteLogSegmentId> map = partitionsWithSegmentIds
                .computeIfAbsent(tp, topicPartition -> new ConcurrentSkipListMap<>());
            if (metadata.markedForDeletion()) {
                db.delete(uuid2bytes(metadata.remoteLogSegmentId().id()));
                // todo-tier check for concurrent updates when leader/follower switches occur
                map.remove(metadata.startOffset());
            } else {
                map.put(metadata.startOffset(), metadata.remoteLogSegmentId());
                db.put(uuid2bytes(metadata.remoteLogSegmentId().id()), serializer.serialize(null, metadata));
            }
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to update remote segment metadata in RocksDB: " + ex.getMessage(), ex);
        }
    }

    @Override
    public NavigableMap<Long, RemoteLogSegmentId> getSegmentIds(TopicPartition tp) {
        return partitionsWithSegmentIds.get(tp);
    }

    @Override
    public RemoteLogSegmentMetadata getMetaData(RemoteLogSegmentId id) {
        try {
            byte[] bytes = db.get(uuid2bytes(id.id()));
            return deserializer.deserialize(null, bytes);
        } catch(RocksDBException ex) {
            throw new KafkaException("Failed to get remote segment metadata from RocksDB: " + ex.getMessage(), ex);
        }
    }

    byte[] uuid2bytes(UUID id) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(id.getMostSignificantBits());
        bb.putLong(id.getLeastSignificantBits());
        return bb.array();
    }
}
