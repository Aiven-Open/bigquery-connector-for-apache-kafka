package com.wepay.kafka.connect.bigquery.strategy;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

public class TestingCommitStrategy implements CommitStrategy {
    Map<KafkaRecordKey, StreamStateContainer> commitMap = new TreeMap<>();
    Collection<TopicPartition> registered = new HashSet<>();

    @Override
    public void queueKeys(Stream<KafkaRecordKey> keys, StreamStateContainer streamStateContainer) {
        CommitStrategy.extractHighKeys(keys).forEach(key -> commitMap.put(key, streamStateContainer));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        Map<TopicPartition, OffsetAndMetadata> result = new LinkedHashMap<>();
        commitMap.keySet().forEach(k -> result.put(k.getTopicPartition(), k.getOffsetAndMetadata()));
        return result;
    }

    /**
     * Gets the sety of keys that were queued.
     * @return the sety of keys that were queued.
     */
    public Set<KafkaRecordKey> getRecordKeys() {
        return commitMap.keySet();
    }

    @Override
    public boolean cleanCommitStrategy() {
        return false;
    }

    @Override
    public void registerPartitions(Collection<TopicPartition> partitions) {
        registered.addAll(partitions);
    }

    @Override
    public void unregisterPartitions(Collection<TopicPartition> partitions) {
        registered.removeAll(partitions);
    }
}
