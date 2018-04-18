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

package com.netflix.sstableadaptor;


import com.netflix.sstableadaptor.sstable.SSTableIterator;
import com.netflix.sstableadaptor.sstable.SSTableSingleReader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *  Tests on reading local sstable and s3 sstable files.
 */
public class TestReadingSSTable3 extends TestBaseSSTableFunSuite {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestReadingSSTable3.class);

    /**
     * Setting up resources prior to running any tests.
     * @throws Exception when we cannot initialize the resources
     */
    @BeforeClass
    public static void setup() throws Exception {
        LOGGER.info("Running TestReadingSSTable3 setup ...");
        TestBaseSSTableFunSuite.setup();
    }

    /**
     * Tear down resources after all tests.
     * @throws Exception when teardown has an issue
     */
    @AfterClass
    public static void teardown() throws Exception {
        LOGGER.info("Tearing TestReadingSSTable3 down ...");
        TestBaseSSTableFunSuite.teardown();
    }

    /**
     *  This test works on the sstable file.
     *      location: src/test/resources/data/cass3/keyspace1/bills_compress/mc-6-big-Data.db
     *
     *   The corresponding table definition is :
     *       CREATE TABLE bills_compress (
     *          user text,
     *          balance int static,
     *          expense_id int,
     *          amount int,
     *          name text,
     *          PRIMARY KEY (user, expense_id)
     *       )
     *
     */
    @Test
    public void testOnLocalDataSimplePartitionKey() {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-6-big-Data.db";
        final int counter = getRowCount(inputSSTableFullPathFileName);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(4, counter);
    }

    /**
     *  This test works on the sstable file.
     *      location: S3_INPUT_DIR/mc-6-big-Data.db
     *
     *   The corresponding table definition is :
     *       CREATE TABLE bills_compress (
     *          user text,
     *          balance int static,
     *          expense_id int,
     *          amount int,
     *          name text,
     *          PRIMARY KEY (user, expense_id)
     *       )
     *   Also needs to run S3 proxy to work around the credential setting
     */
    @Test
    public void testOnS3DataSimplePartitionKey() {
        if (S3_INPUT_DIR == null) {
            LOGGER.info("Skip this test as there is no setting for S3_INPUT_DIR");
            return;
        }

        final String inputSSTableFullPathFileName = S3_INPUT_DIR + "/mc-6-big-Data.db";
        LOGGER.info("Processing on file at: " + inputSSTableFullPathFileName);

        final int counter = getRowCount(inputSSTableFullPathFileName);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(4, counter);
    }


    /**
     *  Test on a local data with composite partition key.
     *  CREATE TABLE compressed_bills (
     *       user text,
     *       email text,
     *       account_id text static,
     *       balance int static,
     *       expense_id int,
     *       item_id int,
     *       amount int,
     *       name text,
     *   PRIMARY KEY ((user, email), expense_id, item_id))
     */
    @Test
    public void testOnLocalDataCompositePartitionKey() {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/compressed_bills/mc-2-big-Data.db";
        final int counter = getRowCount(inputSSTableFullPathFileName);

        LOGGER.info("\nCounter: " + counter);
        Assert.assertEquals(16, counter);
    }

    /**
     * Test on the SSTableIterator.
     * @throws IOException
     */
    @Test
    public void testCasspactorIterator() throws IOException {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader1 = new SSTableSingleReader(inputSSTableFullPathFileName,
                                                                   TestBaseSSTableFunSuite.HADOOP_CONF);
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName,
                                                                    TestBaseSSTableFunSuite.HADOOP_CONF);
        final CFMetaData cfMetaData = reader1.getCfMetaData();
        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader1.getSSTableScanner());
        scanners.add(reader2.getSSTableScanner());

        int counter = 0;
        try (SSTableIterator ci = new SSTableIterator(scanners, reader1.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                counter += printRowDetails(cfMetaData, rowIterator, false);
            }
        }

        Assert.assertEquals(4, counter);
    }

    /**
     * Test on the Original and Change Flag setting when we only have the original file(s).
     * Note that original file must be downloaded and stored under a director path which has "casspactor" string
     * @throws IOException
     */
    @Test
    public void testCasspactorOriginalFlagOnOriginalFileOnly() throws IOException {

        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "casspactor_temp/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader.getSSTableScanner());

        try (SSTableIterator ci = new SSTableIterator(scanners, reader.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                final Row staticRow = rowIterator.staticRow();
                Assert.assertEquals(true, staticRow.isOriginal());
                Assert.assertEquals(true, staticRow.hasChanged());
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    Assert.assertEquals(true, row.isOriginal());
                    Assert.assertEquals(true, row.hasChanged());
                }
            }
        }

        //Two identical files
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);
        scanners.add(reader2.getSSTableScanner());
        try (SSTableIterator ci = new SSTableIterator(scanners, reader.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                final Row staticRow = rowIterator.staticRow();
                Assert.assertEquals(true, staticRow.isOriginal());
                Assert.assertEquals(true, staticRow.hasChanged());
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    Assert.assertEquals(true, row.isOriginal());
                    Assert.assertEquals(true, row.hasChanged());
                }
            }
        }
    }


    /**
     * Test on the Original and Change Flag setting when we only have the previously processed file.
     * Note that processed file is normally stored on S3 or some storage and its directory path does not
     * contains "casspactor" string.
     * @throws IOException
     */
    @Test
    public void testCasspactorOriginalFlagOnProcessedFileOnly() throws IOException {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader.getSSTableScanner());
        scanners.add(reader2.getSSTableScanner());

        try (SSTableIterator ci = new SSTableIterator(scanners, reader.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                final Row staticRow = rowIterator.staticRow();
                Assert.assertEquals(false, staticRow.isOriginal());
                Assert.assertEquals(false, staticRow.hasChanged());
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    Assert.assertEquals(false, row.isOriginal());
                    Assert.assertEquals(false, row.hasChanged());
                }
            }
        }
    }


    /**
     * Test on the Original and Change Flag setting when we have a mixed of both original and processed files and they
     * both have the same content.
     * @throws IOException
     */
    @Test
    public void testCasspactorOriginalFlagOnMixingWithSameContent() throws IOException {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader1 = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final String inputSSTableFullPathFileName2 = CASS3_DATA_DIR + "casspactor_temp/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName2,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader1.getSSTableScanner());
        scanners.add(reader2.getSSTableScanner());

        try (SSTableIterator ci = new SSTableIterator(scanners, reader1.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                final Row staticRow = rowIterator.staticRow();
                Assert.assertEquals(true, staticRow.isOriginal());
                Assert.assertEquals(false, staticRow.hasChanged());
                while (rowIterator.hasNext()) {
                    final Row row = rowIterator.next();
                    Assert.assertEquals(true, row.isOriginal());
                    Assert.assertEquals(false, row.hasChanged());
                }
            }
        }
    }

    /**
     * Test on the Original and Change Flag setting when we have a mixed of both original and processed files which
     * have the different content on different disjoint set of keys.
     * @throws IOException
     */
    @Test
    public void testCasspactorOriginalFlagOnMixingWithDifferentContentOnDifferentKeys() throws IOException {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-2-big-Data.db";
        final SSTableSingleReader reader1 = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final String inputSSTableFullPathFileName2 = CASS3_DATA_DIR + "casspactor_temp/bills_compress/mc-6-big-Data.db";
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName2,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader1.getSSTableScanner());
        scanners.add(reader2.getSSTableScanner());

        try (SSTableIterator ci = new SSTableIterator(scanners, reader1.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                String partitionKey = new String(rowIterator.partitionKey().getKey().array());
                final Row staticRow = rowIterator.staticRow();
                System.out.println("partitionKey: " + partitionKey);
                if (partitionKey.equals("user11") || partitionKey.equals("user12")) {
                    Assert.assertEquals(false, staticRow.isOriginal());
                    Assert.assertEquals(false, staticRow.hasChanged());
                    while (rowIterator.hasNext()) {
                        final Row row = rowIterator.next();
                        Assert.assertEquals(false, row.isOriginal());
                        Assert.assertEquals(false, row.hasChanged());
                    }
                } else {
                    Assert.assertEquals(true, staticRow.isOriginal());
                    Assert.assertEquals(true, staticRow.hasChanged());
                    while (rowIterator.hasNext()) {
                        final Row row = rowIterator.next();
                        Assert.assertEquals(true, row.isOriginal());
                        Assert.assertEquals(true, row.hasChanged());
                    }
                }
            }
        }
    }

    /**
     * Test on the Original and Change Flag setting when we have a mixed of both original and processed files which
     * have the different content on same set of keys.
     * @throws IOException
     */
    @Test
    public void testCasspactorOriginalFlagOnMixingWithDifferentContentOnSameKeys() throws IOException {
        final String inputSSTableFullPathFileName = CASS3_DATA_DIR + "keyspace1/bills_compress/mc-2-big-Data.db";
        final SSTableSingleReader reader1 = new SSTableSingleReader(inputSSTableFullPathFileName,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final String inputSSTableFullPathFileName2 = CASS3_DATA_DIR + "casspactor_temp/bills_compress/mc-3-big-Data.db";
        final SSTableSingleReader reader2 = new SSTableSingleReader(inputSSTableFullPathFileName2,
                TestBaseSSTableFunSuite.HADOOP_CONF);

        final List<ISSTableScanner> scanners = new ArrayList<>();
        final int nowInSecs = (int) (System.currentTimeMillis() / 1000);

        scanners.add(reader1.getSSTableScanner());
        scanners.add(reader2.getSSTableScanner());

        try (SSTableIterator ci = new SSTableIterator(scanners, reader1.getCfMetaData(), nowInSecs)) {
            while (ci.hasNext()) {
                final RowIterator rowIterator = ci.next();
                String partitionKey = new String(rowIterator.partitionKey().getKey().array());
                final Row staticRow = rowIterator.staticRow();

                if (partitionKey.equals("user12")) {
                    Assert.assertEquals(true, staticRow.isOriginal());
                    Assert.assertEquals(true, staticRow.hasChanged());
                    while (rowIterator.hasNext()) {
                        final Row row = rowIterator.next();
                        Assert.assertEquals(true, row.isOriginal());
                        Assert.assertEquals(true, row.hasChanged());
                    }
                }
            }
        }
    }


}
