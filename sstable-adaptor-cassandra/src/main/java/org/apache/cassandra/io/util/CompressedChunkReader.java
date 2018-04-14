/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

public abstract class CompressedChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    final CompressionMetadata metadata;

    protected CompressedChunkReader(ChannelProxy channel, CompressionMetadata metadata)
    {
        super(channel, metadata.dataLength);
        this.metadata = metadata;
        assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
    }

    @VisibleForTesting
    public double getCrcCheckChance()
    {
        return metadata.parameters.getCrcCheckChance();
    }

    @Override
    public String toString()
    {
        return String.format("CompressedChunkReader.%s(%s - %s, chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             metadata.compressor().getClass().getSimpleName(),
                             metadata.chunkLength(),
                             metadata.dataLength);
    }

    @Override
    public int chunkSize()
    {
        return metadata.chunkLength();
    }

    @Override
    public BufferType preferredBufferType()
    {
        return metadata.compressor().preferredBufferType();
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Aligned(this);
    }

    public static class Standard extends CompressedChunkReader
    {
        // we read the raw compressed bytes into this buffer, then uncompressed them into the provided one.
        private final ThreadLocal<ByteBuffer> compressedHolder;

        public Standard(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);
            compressedHolder = ThreadLocal.withInitial(this::allocateBuffer);
        }

        public ByteBuffer allocateBuffer()
        {
            return allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));
        }

        public ByteBuffer allocateBuffer(int size)
        {
            return metadata.compressor().preferredBufferType().allocate(size);
        }

        @Override
        public void readChunk(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
                ByteBuffer compressed = compressedHolder.get();

                if (compressed.capacity() < chunk.length)
                {
                    compressed = allocateBuffer(chunk.length);
                    compressedHolder.set(compressed);
                }
                else
                {
                    compressed.clear();
                }

                compressed.limit(chunk.length);
                //TODO: will make the retry look nicer with an abstraction class
                int attempt = 0;
                int maxAttempt = 5;
                boolean isSuccess = false;
                while (!isSuccess)
                {
                    if (attempt > 0)
                        FBUtilities.sleepQuietly((int) Math.round(Math.pow(2, attempt)) * 1000);

                    try {
                       int numByteRead = channel.read(compressed, chunk.offset);
                       if (numByteRead != chunk.length)
                           throw new CorruptBlockException(channel.filePath(), chunk);
                        isSuccess = true;
                    }
                    catch (IOException e)
                    {
                        channel.reopenInputStream();
                        if (attempt == maxAttempt) {
                            //TODO: what if this is still a network issue, not data corruption
                            throw new CorruptSSTableException(e, "Error on reading " + channel.filePath() +
                              " on num. attempt " + maxAttempt + " at position " + chunk.offset);
                        }
                        attempt++;
                    }
                }

                compressed.flip();
                uncompressed.clear();

                try
                {
                    metadata.compressor().uncompress(compressed, uncompressed);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk, e);
                }
                finally
                {
                    uncompressed.flip();
                }

                /** //TODO: Add this back later
                if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
                {
                    compressed.rewind();
                    int checksum = (int) metadata.checksumType.of(compressed);

                    compressed.clear().limit(Integer.BYTES);
                    //TODO: need to retry here? Or skip this checksum entirely
                    if (channel.read(compressed, chunk.offset + chunk.length) != Integer.BYTES
                                || compressed.getInt(0) != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }
                */
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }
    }


}
