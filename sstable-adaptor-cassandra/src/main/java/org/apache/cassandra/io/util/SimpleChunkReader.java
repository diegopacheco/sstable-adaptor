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

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.FBUtilities;

import java.nio.ByteBuffer;

class SimpleChunkReader extends AbstractReaderFileProxy implements ChunkReader
{
    private final int bufferSize;
    private final BufferType bufferType;

    SimpleChunkReader(ChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        super(channel, fileLength);
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
    }

    @Override
    public void readChunk(long position, ByteBuffer buffer)
    {
        buffer.clear();

        //TODO: will make the retry look nicer with an abstraction class
        int attempt = 0;
        int maxAttempt = 3;
        boolean isSuccess = false;
        while (!isSuccess) {
            if (attempt > 0)
               FBUtilities.sleepQuietly((int) Math.round(Math.pow(2, attempt)) * 1000);
            try {
                channel.read(buffer, position);
                isSuccess = true;
            }
            catch (Exception e)
            {
               channel.reopenInputStream();
               if (attempt == maxAttempt) {
                  //TODO: what if this is still a network issue, not data corruption
                  throw new FSReadError(e, "Error on reading " + channel.filePath() +
                        " on num. attempt " + maxAttempt + " at position " + position);
               }
            }
            finally
            {
                attempt++;
            }
        }
        buffer.flip();
    }

    @Override
    public int chunkSize()
    {
        return bufferSize;
    }

    @Override
    public BufferType preferredBufferType()
    {
        return bufferType;
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return new BufferManagingRebufferer.Unaligned(this);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             bufferSize,
                             fileLength());
    }
}