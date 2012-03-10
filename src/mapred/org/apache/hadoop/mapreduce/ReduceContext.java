/**
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

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.SailfishSerialization;
import org.apache.hadoop.util.Progressable;

/**
 * The context passed to the {@link Reducer}.
 * @param <KEYIN> the class of the input keys
 * @param <VALUEIN> the class of the input values
 * @param <KEYOUT> the class of the output keys
 * @param <VALUEOUT> the class of the output values
 */
public class ReduceContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends TaskInputOutputContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  private RawKeyValueIterator input;
  private Counter inputKeyCounter;
  private Counter inputValueCounter;
  private RawComparator<KEYIN> comparator;
  private KEYIN key;                                  // current key
  private VALUEIN value;                              // current value
  private boolean firstValue = false;                 // first value in key
  private boolean nextKeyIsSame = false;              // more w/ this key
  private boolean hasMore;                            // more in file
  protected Progressable reporter;
  private Deserializer<KEYIN> keyDeserializer;
  private Deserializer<VALUEIN> valueDeserializer;
  private DataInputBuffer buffer = new DataInputBuffer();
  private BytesWritable currentRawKey = new BytesWritable();
  private ValueIterable iterable = new ValueIterable();

  @SuppressWarnings("unchecked")
  public ReduceContext(Configuration conf, TaskAttemptID taskid,
                       RawKeyValueIterator input, 
                       Counter inputKeyCounter,
                       Counter inputValueCounter,
                       RecordWriter<KEYOUT,VALUEOUT> output,
                       OutputCommitter committer,
                       StatusReporter reporter,
                       RawComparator<KEYIN> comparator,
                       Class<KEYIN> keyClass,
                       Class<VALUEIN> valueClass
                       ) throws InterruptedException, IOException{
    super(conf, taskid, output, committer, reporter);
    this.input = input;
    this.inputKeyCounter = inputKeyCounter;
    this.inputValueCounter = inputValueCounter;
    
    SerializationFactory serializationFactory = new SerializationFactory(conf);
    
    if (conf.getBoolean("sailfish.mapred.job.use_ifile", false)) {
      @SuppressWarnings("rawtypes")
      SailfishSerialization sfs = new SailfishSerialization();
      this.keyDeserializer = (Deserializer<KEYIN>) sfs.getDeserializer(keyClass);
      this.comparator = SailfishSerialization.isPigKeyClass(keyClass)
        ? new Pigcmp()
        : new Memcmp();
    } else {
      this.keyDeserializer = serializationFactory.getDeserializer(keyClass);
      this.comparator = comparator;
    }
    this.keyDeserializer.open(buffer);
    this.valueDeserializer = serializationFactory.getDeserializer(valueClass);
    this.valueDeserializer.open(buffer);
    hasMore = input.next();
  }

  private class Pigcmp implements RawComparator<KEYIN> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      if (b1[0] == 0 && b2[0] == 0) {
        // both non-null; ignore index byte
        return WritableComparator.compareBytes(b1, s1, l1 - 1, b2, s2, l2 - 1);
      }
      if (b1[0] != 0 && b2[0] != 0) {
        // both null; compare by index
        final byte idxSpace = 0x7F;
        if ((b1[1] & idxSpace) < (b2[1] & idxSpace)) return -1;
        else if ((b1[1] & idxSpace) > (b2[1] & idxSpace)) return 1;
        else return 0;
      }
      if (b1[0] != 0) {
        return -1;
      }
      return 1;
    }
    @Override
    public int compare(KEYIN k1, KEYIN k2) {
      throw new UnsupportedOperationException("Internal error");
    }
    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException("Internal error");
    }
  }
  
  private class Memcmp implements RawComparator<KEYIN> {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
    }
    @Override
    public int compare(KEYIN k1, KEYIN k2) {
      throw new UnsupportedOperationException("Internal error");
    }
    @Override
    public boolean equals(Object o) {
      throw new UnsupportedOperationException("Internal error");
    }
  }

  /** Start processing next unique key. */
  public boolean nextKey() throws IOException,InterruptedException {
    while (hasMore && nextKeyIsSame) {
      nextKeyValue();
    }
    if (hasMore) {
      if (inputKeyCounter != null) {
        inputKeyCounter.increment(1);
      }
      return nextKeyValue();
    } else {
      return false;
    }
  }

  /**
   * Advance to the next key/value pair.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!hasMore) {
      key = null;
      value = null;
      return false;
    }
    firstValue = !nextKeyIsSame;
    DataInputBuffer next = input.getKey();
    currentRawKey.set(next.getData(), next.getPosition(), 
                      next.getLength() - next.getPosition());
    buffer.reset(currentRawKey.getBytes(), 0, currentRawKey.getLength());
    key = keyDeserializer.deserialize(key);
    next = input.getValue();
    buffer.reset(next.getData(), next.getPosition(), next.getLength());
    value = valueDeserializer.deserialize(value);
    hasMore = input.next();
    if (hasMore) {
      next = input.getKey();
      nextKeyIsSame = comparator.compare(currentRawKey.getBytes(), 0, 
                                         currentRawKey.getLength(),
                                         next.getData(),
                                         next.getPosition(),
                                         next.getLength() - next.getPosition()
                                         ) == 0;
    } else {
      nextKeyIsSame = false;
    }
    inputValueCounter.increment(1);
    return true;
  }

  public KEYIN getCurrentKey() {
    return key;
  }

  @Override
  public VALUEIN getCurrentValue() {
    return value;
  }

  protected class ValueIterator implements Iterator<VALUEIN> {

    @Override
    public boolean hasNext() {
      return firstValue || nextKeyIsSame;
    }

    @Override
    public VALUEIN next() {
      // if this is the first record, we don't need to advance
      if (firstValue) {
        firstValue = false;
        return value;
      }
      // if this isn't the first record and the next key is different, they
      // can't advance it here.
      if (!nextKeyIsSame) {
        throw new NoSuchElementException("iterate past last value");
      }
      // otherwise, go to the next key/value pair
      try {
        nextKeyValue();
        return value;
      } catch (IOException ie) {
        throw new RuntimeException("next value iterator failed", ie);
      } catch (InterruptedException ie) {
        // this is bad, but we can't modify the exception list of java.util
        throw new RuntimeException("next value iterator interrupted", ie);        
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not implemented");
    }
    
  }

  protected class ValueIterable implements Iterable<VALUEIN> {
    private ValueIterator iterator = new ValueIterator();
    @Override
    public Iterator<VALUEIN> iterator() {
      return iterator;
    } 
  }
  
  /**
   * Iterate through the values for the current key, reusing the same value 
   * object, which is stored in the context.
   * @return the series of values associated with the current key. All of the 
   * objects returned directly and indirectly from this method are reused.
   */
  public 
  Iterable<VALUEIN> getValues() throws IOException, InterruptedException {
    return iterable;
  }
}
