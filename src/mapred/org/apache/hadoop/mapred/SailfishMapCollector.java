/**
 * Copyright 2010 Yahoo Corporation.  All rights reserved.
 * This file is part of the Sailfish project.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */
package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_BYTES;
import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Task;

/**
 * collect the mapper output and flush it to KAppender
 * @author sriramr
 *
 * @param <K>
 * @param <V>
 */
public class SailfishMapCollector<K extends Object, V extends Object> implements
    org.apache.hadoop.mapred.MapTask.MapOutputCollector<K, V> {

  static final private Log LOG = LogFactory.getLog(SailfishMapCollector.class);
  BufferedOutputStream kappenderStdin;
  private final Reporter reporter;
  private final Class<K> keyClass;
  private final Class<V> valClass;
  private final SerializationFactory serializationFactory;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valSerializer;
  private final Counters.Counter mapOutputByteCounter;
  private final Counters.Counter mapOutputRecordCounter;
  private final SailfishMapOutputBuffer keyOutputBuffer;
  private final SailfishMapOutputBuffer valOutputBuffer;
  private ByteBuffer bb;
  private byte[] packet;
  private boolean shouldSendKey = false;

  @SuppressWarnings({ "unchecked", "deprecation" })
  public SailfishMapCollector(JobConf hadoopJob, Reporter tr)
      throws IOException {
    reporter = tr;
    bb = ByteBuffer.allocate(4096);
    packet = new byte[4096];
    keyOutputBuffer = new SailfishMapOutputBuffer();
    valOutputBuffer = new SailfishMapOutputBuffer();
    keyClass = (Class<K>) hadoopJob.getMapOutputKeyClass();
    valClass = (Class<V>) hadoopJob.getMapOutputValueClass();
    serializationFactory = new SerializationFactory(hadoopJob);
    SailfishSerialization sfs = new SailfishSerialization();
    keySerializer = (Serializer<K>) sfs.getSerializer(keyClass);
    // keySerializer = serializationFactory.getSerializer(keyClass);
    keySerializer.open(keyOutputBuffer);
    valSerializer = serializationFactory.getSerializer(valClass);
    valSerializer.open(valOutputBuffer);
    mapOutputByteCounter = reporter.getCounter(MAP_OUTPUT_BYTES);
    mapOutputRecordCounter = reporter.getCounter(MAP_OUTPUT_RECORDS);
    // shouldSendKey = hadoopJob.getBoolean("sailfish.mapred.job.send_key", false);
  }

  public void setOutputStream(OutputStream os) {
    kappenderStdin = new BufferedOutputStream(os, 4096);
  }

  /**
   * Collect a record and send it down to KAppender
   */
  @Override
  public void collect(K key, V value, int partition) throws IOException,
      InterruptedException {
    int keyLen, valLen, tKeyLen = 0;
    int pktSize, totalSize;
    keySerializer.serialize(key);
    valSerializer.serialize(value);
    keyLen = keyOutputBuffer.size();
    valLen = valOutputBuffer.size();
    // in the I-file, the K/V will be laid out as: <pktSize><key len><key><data
    // len><data>
    pktSize = keyLen + valLen + 4 + 4;
    // total bytes to kappender:
    //  payload: pktSize + 4 bytes for partition # + 4 bytes for total size
    //  key: 4 bytes for keylength and the bytes for the key
    if (shouldSendKey)
      tKeyLen = keyLen;
    totalSize = pktSize + 8 + 4 + tKeyLen;
    bb.clear();
    if (bb.capacity() < totalSize) {
      // grow the buffer if needed
      bb = ByteBuffer.allocate(totalSize + 4096);
      packet = new byte[totalSize + 4096];
      bb.clear();
    }
    // want to skew the data...
    /*
    if ((partition % 10 == 0) && (20 <= partition) && (partition <= 100))
      partition++;
    */
    bb.putInt(partition);
    bb.putInt(tKeyLen);
    bb.putInt(pktSize);
    if (tKeyLen > 0) {
      // XXX: Not needed anymore. The chunksorter knows how to parse out the key
      // from a given record and build the index. Format of a record is defined
      // above.
      bb.put(keyOutputBuffer.getByteArray(), 0, tKeyLen);
    }
    // payload
    bb.putInt(keyLen);
    bb.put(keyOutputBuffer.getByteArray(), 0, keyLen);
    bb.putInt(valLen);
    bb.put(valOutputBuffer.getByteArray(), 0, valLen);
    bb.flip();

    /*
      LOG.info("Data for partition: " + partition + " size = " + pktSize +
      " totalsize = " + totalSize + " key size: " + keyLen + " val size: " +
      valLen);
    */
    bb.get(packet, 0, totalSize);
    kappenderStdin.write(packet, 0, totalSize);
    mapOutputRecordCounter.increment(1);
    mapOutputByteCounter.increment(pktSize - 8);
    keyOutputBuffer.reset();
    valOutputBuffer.reset();
  }

  @Override
  /**
   * Tell KAppender to flush and shutdown.
   */
  public void close() throws IOException {
    // send the close sequence: bogus partition followed by 0-byte key and 4 bytes: 0xDEADDEAD
    // telling the appender to exit
    int totalSize = 16;
    bb.clear();
    bb.putInt(-1);
    // key length == 0
    bb.putInt(0);
    bb.putInt(4);
    bb.putInt(0xDEADDEAD);
    bb.flip();
    bb.get(packet, 0, totalSize);
    kappenderStdin.write(packet, 0, totalSize);
    kappenderStdin.flush();
    LOG.info("All done...sending shutdown sequence is done");
  }

  @Override
  public void flush() throws IOException, InterruptedException {
    kappenderStdin.flush();
  }

}
