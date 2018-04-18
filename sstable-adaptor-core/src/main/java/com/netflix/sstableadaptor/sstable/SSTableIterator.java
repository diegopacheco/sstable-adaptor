/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.sstableadaptor.sstable;


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * Borrow this from Cassandra's code base.
 *
 * Merge multiple iterators over the content of sstable into a "compacted" iterator.
 * <p>
 * On top of the actual merging the source iterators, this class:
 * <ul>
 * <li>purge gc-able tombstones if possible (see PurgeIterator below).</li>
 * </ul>
 *
 */
public class SSTableIterator implements PartitionIterator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SSTableIterator.class);
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final List<ISSTableScanner> scanners;
    private final int nowInSec;

    private final long totalBytes;
    private long bytesRead;
    private long totalSourceCQLRows;
    private long uniqKeys;
    private long deletedRows;
    private CFMetaData cfMetaData;

    /**
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] mergeCounters;
    private final PartitionIterator compacted;

    /**
     * We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable.
     *
     * @param scanners   - a list of sstable scanners
     * @param cfMetaData - CF Metadata for those sstables
     * @param nowInSec   - now in secs
     */
    @SuppressWarnings("resource")
    public SSTableIterator(final List<ISSTableScanner> scanners, final CFMetaData cfMetaData, final int nowInSec)
    {
        this.cfMetaData = cfMetaData;
        this.scanners = scanners;
        this.nowInSec = nowInSec;
        this.bytesRead = 0;

        long bytes = 0;
        for (ISSTableScanner scanner : scanners) {
            bytes += scanner.getLengthInBytes();
        }

        this.totalBytes = bytes;
        this.mergeCounters = new long[scanners.size()];

        final UnfilteredPartitionIterator merged = scanners.isEmpty()
                ? EmptyIterators.unfilteredPartition(cfMetaData, false)
                : UnfilteredPartitionIterators.merge(scanners, nowInSec, listener());

        UnfilteredPartitionIterator mergeAndPurge = Transformation.apply(merged, new Purger(nowInSec));
        compacted = UnfilteredPartitionIterators.filter(mergeAndPurge, nowInSec);
    }

    /**
     * Is this a thrift data.
     *
     * @return true/false
     */
    public boolean isForThrift()
    {
        return false;
    }

    /**
     * Return CF Metadata.
     *
     * @return CFMetaData
     */
    public CFMetaData metadata()
    {
        return this.cfMetaData;
    }


    private void updateCounterFor(final int rows)
    {
        assert rows > 0 && rows - 1 < mergeCounters.length;
        mergeCounters[rows - 1] += 1;
    }

    private void updateUniqKeys(final int count)
    {
        uniqKeys += count;
    }

    /**
     * Return total merged rows.
     *
     * @return long array
     */
    public long[] getMergedRowCounts()
    {
        return mergeCounters;
    }

    /**
     * Return total raw rows.
     *
     * @return long
     */
    public long getTotalSourceCQLRows()
    {
        return totalSourceCQLRows;
    }

    /**
     * Return read bytes.
     * @return long
     */
    public long getBytesRead()
    {
        return bytesRead;
    }

    private boolean isEmpty(Row row)
    {
        return row == null || row.isEmpty();
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener() {
            public UnfilteredRowIterators.MergeListener
            getRowMergeListener(final DecoratedKey partitionKey,
                                final List<UnfilteredRowIterator> versions)
            {
                int merged = 0;
                for (UnfilteredRowIterator iter : versions) {
                    if (iter != null) {
                        merged++;
                    }
                }

                assert merged > 0;
                SSTableIterator.this.updateCounterFor(merged);
                return new UnfilteredRowIterators.MergeListener() {
                    public void onMergedPartitionLevelDeletion(final DeletionTime mergedDeletion,
                                                               final DeletionTime[] versions)
                    {
                        deletedRows += 1;
                    }

                    public void onMergedRows(final Row merged, final Row[] versions) {
                        SSTableIterator.this.updateUniqKeys(1);

                        if (versions.length > 1) {
                            boolean isOriginal = false;
                            for (Row row : versions) {
                                if (!isEmpty(row) && row.isOriginal()) {
                                    isOriginal = true;
                                    break;
                                }
                            }
                            merged.setOriginal(isOriginal);
                        }

                        int counter = 0;
                        boolean hasOneOriginal = false;
                        for(Row row: versions) {
                            if (isEmpty(row))
                                continue;
                            counter++;
                            if (row.isOriginal())
                                hasOneOriginal = true;
                        }

                        if (counter == 1) {
                            merged.setChanged(hasOneOriginal);
                        } else {
                            boolean isChanged = false;
                            for(Row row: versions) {
                                if (isEmpty(row))
                                    continue;

                                if (row.isOriginal() && row.dataSize() != merged.dataSize()) {
                                    isChanged = true;
                                    break;
                                }
                            }

                            //double check on timestamps when all sizes are the same
                            if (!isChanged) {
                                long mergedRowTimestampTotal = 0;
                                Iterator<ColumnData> mergedColumnDataIterator = merged.iterator();
                                while (mergedColumnDataIterator.hasNext()) {
                                    mergedRowTimestampTotal += mergedColumnDataIterator.next().maxTimestamp();
                                }

                                for(Row row: versions) {
                                    if (isEmpty(row))
                                        continue;
                                    long rowTimestampTotal = 0;
                                    Iterator<ColumnData> columnDataIterator = row.iterator();
                                    while (columnDataIterator.hasNext())
                                        rowTimestampTotal += columnDataIterator.next().maxTimestamp();

                                    if (mergedRowTimestampTotal != rowTimestampTotal && merged != null) {
                                        isChanged = true;
                                        break;
                                    }
                                }
                            }
                            merged.setChanged(isChanged);
                        }
                    }

                    public void onMergedRangeTombstoneMarkers(final RangeTombstoneMarker mergedMarker,
                                                              final RangeTombstoneMarker[] versions)
                    {
                    }

                    public void close()
                    {
                    }
                };
            }

            public void close() {
            }
        };
    }

    private void updateBytesRead()
    {
        long n = 0;
        for (ISSTableScanner scanner : scanners) {
            n += scanner.getCurrentPosition();
        }
        bytesRead = n;
    }

    /**
     * Have a next item.
     *
     * @return true/false
     */
    public boolean hasNext()
    {
        return compacted.hasNext();
    }

    /**
     * Return next item.
     *
     * @return RowIterator
     */
    public RowIterator next()
    {
        return compacted.next();
    }

    /**
     * Unsupported.
     */
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * close the underneath iterator.
     */
    public void close()
    {
        try {
            compacted.close();
            for (ISSTableScanner scanner : scanners) {
                scanner.close();
            }
        } catch (Exception e) {
            LOGGER.info(e.getMessage());
        }
    }

    private final class Purger extends PurgeFunction
    {
        private DecoratedKey currentKey;
        private long maxPurgeableTimestamp;
        private boolean hasCalculatedMaxPurgeableTimestamp;

        private long compactedUnfiltered;

        private Purger(final int nowInSec)
        {
            super(false, nowInSec, nowInSec, Integer.MAX_VALUE, true);
        }

        @Override
        protected void onEmptyPartitionPostPurge(final DecoratedKey key)
        {
        }

        @Override
        protected void onNewPartition(final DecoratedKey key)
        {
            currentKey = key;
            hasCalculatedMaxPurgeableTimestamp = false;
        }

        @Override
        protected void updateProgress()
        {
            totalSourceCQLRows++;
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0) {
                updateBytesRead();
            }
        }

        /**
         * Tombstones with a localDeletionTime before this can be purged. This is the minimum timestamp for any sstable
         * containing `currentKey` outside of the set of sstables involved in this compaction. This is computed lazily
         * on demand as we only need this if there is tombstones and this a bit expensive (see #8914).
         */
        protected long getMaxPurgeableTimestamp()
        {
            if (!hasCalculatedMaxPurgeableTimestamp) {
                hasCalculatedMaxPurgeableTimestamp = true;
                maxPurgeableTimestamp = Long.MAX_VALUE; //Todo: will make it usable
            }
            return maxPurgeableTimestamp;
        }
    }

}
