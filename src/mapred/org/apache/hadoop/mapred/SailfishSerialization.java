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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

public class SailfishSerialization<K extends BinaryComparable> extends Configured implements Serialization<K> {

  private static final Class<?> pigNullableBytesWritableClass = pigNullableBytesWritableType();
  private static final Class<?> pigNullableTupleClass = pigNullableTupleType();
  private static final Class<?> pigNullableTextClass = pigNullableTextType();

  public static Class<?> pigNullableBytesWritableType() {
    try {
      return Class.forName("org.apache.pig.impl.io.NullableBytesWritable");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
  public static Class<?> pigNullableTupleType() {
    try {
      return Class.forName("org.apache.pig.impl.io.NullableTuple");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
  public static Class<?> pigNullableTextType() {
    try {
      return Class.forName("org.apache.pig.impl.io.NullableText");
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
  
  static class SailfishSerializer<T extends BinaryComparable> implements Serializer<T> {

    private DataOutputStream dataOut;

    public void open(OutputStream out) {
      if (out instanceof DataOutputStream) {
        dataOut = (DataOutputStream) out;
      } else {
        dataOut = new DataOutputStream(out);
      }
    }

    public void serialize(T w) throws IOException {
      dataOut.write(w.getBytes(), 0, w.getLength());
    }

    public void close() throws IOException {
      dataOut.close();
    }
  }

  // XXX Grotesque hack
  static class PigSailfishSerializer<K> implements Serializer<K> {
    private final Field mValue;
    private final Field mIndex;
    private final Field mNull;
    private final Class<?> keyClass;
    private DataOutputStream dataOut;
    public PigSailfishSerializer() {
      if (null == pigNullableBytesWritableClass) {
        throw new RuntimeException("Pig classes not found on classpath");
      }

      try {
        keyClass =
          Class.forName("org.apache.pig.impl.io.PigNullableWritable");
        mValue = keyClass.getDeclaredField("mValue");
        mValue.setAccessible(true);
        mIndex = keyClass.getDeclaredField("mIndex");
        mIndex.setAccessible(true);
        mNull = keyClass.getDeclaredField("mNull");
        mNull.setAccessible(true);
      } catch (Exception e) {
        throw new RuntimeException("Internal error", e);
      }
    }

    @Override
    public void open(OutputStream out) {
      if (out instanceof DataOutputStream) {
        dataOut = (DataOutputStream) out;
      } else {
        dataOut = new DataOutputStream(out);
      }
    }
    
    @Override
    public void serialize(K w) throws IOException {
      if (!keyClass.isAssignableFrom(w.getClass())) {
        throw new IOException("Expected " + keyClass + " found " + w.getClass());
      }
      try {
        // may throw ClassCastException if contract not satisfied
        // extract wrapped BinaryComparable value from Pig key type
        boolean bNull = (Boolean)mNull.get(w);
        dataOut.writeBoolean(bNull);
        if (!bNull) {
          BinaryComparable bc = (BinaryComparable) mValue.get(w);
          dataOut.write(bc.getBytes(), 0, bc.getLength());
        }
        dataOut.writeByte((Byte) mIndex.get(w));
      } catch (IllegalAccessException e) {
        throw new IOException("Internal error", e);
      }
    }
    
    @Override
    public void close() throws IOException {
      dataOut.close();
    }
  }
  
  static class SailfishDeserializer<T extends BinaryComparable> extends Configured implements Deserializer<T> {

    private DataInputBuffer dataIn;
    private Class<? extends T> writableClass;

    public SailfishDeserializer(Configuration conf, Class<? extends T> c) {
      setConf(conf);
      this.writableClass = c;
    }

    public void open(InputStream in) {
      if (in instanceof DataInputBuffer) {
        dataIn = (DataInputBuffer) in;
      }
    }

    public T deserialize(T w) throws IOException {
      if (dataIn == null)
        throw new IOException("dataIn is not initialized");

      T writable;
      if (w == null) {
        writable
        = (T) ReflectionUtils.newInstance(writableClass, getConf());
      } else {
        writable = w;
      }
      // the data is in the buffer
      writable.set(dataIn.getData(), 0, dataIn.getLength());
      return writable;
    }

    public void close() throws IOException {
      dataIn.close();
    }

  }
  
  static class PigSailfishDeserializer<T> extends Configured implements Deserializer<T> {
    private final Field mValue;
    private final Field mIndex;
    private final Field mNull;

    private final Class<T> keyClass;
    private final Class<T> actualKeyClass;
    
    private DataInputBuffer dataIn;
    
    @SuppressWarnings("unchecked")
    public PigSailfishDeserializer(Configuration conf, Class<T> c) {
      if (null == pigNullableBytesWritableClass) {
        throw new RuntimeException("Pig classes not found on classpath");
      }

      try {
        keyClass = (Class<T>)
          Class.forName("org.apache.pig.impl.io.PigNullableWritable");
        mValue = keyClass.getDeclaredField("mValue");
        mValue.setAccessible(true);
        mIndex = keyClass.getDeclaredField("mIndex");
        mIndex.setAccessible(true);
        mNull = keyClass.getDeclaredField("mNull");
        mNull.setAccessible(true);
        actualKeyClass = c;
      } catch (Exception e) {
        throw new RuntimeException("Internal error", e);
      }
    }
    
    public void open(InputStream in) {
      if (in instanceof DataInputBuffer) {
        dataIn = (DataInputBuffer) in;
      }
    }

    public T deserialize(T w) throws IOException {
      if (dataIn == null)
        throw new IOException("dataIn is not initialized");

      T writable;
      if (w == null) {
        writable
        = (T) ReflectionUtils.newInstance(actualKeyClass, getConf());
      } else {
        writable = w;
      }
      if (!keyClass.isAssignableFrom(writable.getClass())) {
        throw new IOException("Expected " + keyClass + " found " + writable.getClass());
      }
 
      // TODO inner type
      // the data is in the buffer
      try {
        boolean bNull = dataIn.readBoolean();
        mNull.set(writable, bNull);
        if (!bNull) {
          BinaryComparable bc = (BinaryComparable) mValue.get(writable);
          bc.set(dataIn.getData(), dataIn.getPosition(), dataIn.getLength() - 1);
        }
        mIndex.set(writable, dataIn.readByte());
      } catch (IllegalAccessException e) {
        throw new IOException("Internal error", e);
      }
      return writable;
    }

    public void close() throws IOException {
      dataIn.close();
    }
  }


  
  @Override
  public boolean accept(Class<?> c) {
    return c.isAssignableFrom(BinaryComparable.class) ||
           isPigKeyClass(c);
  }

  public static boolean isPigKeyClass(Class<?> c) {
    return (pigNullableBytesWritableClass != null && 
        (pigNullableBytesWritableClass.isAssignableFrom(c) ||
            pigNullableTextClass.isAssignableFrom(c) ||
            pigNullableTupleClass.isAssignableFrom(c) ));
  }
  @Override
  public Serializer<K> getSerializer(Class<K> c) {
    return isPigKeyClass(c)
      ? new PigSailfishSerializer<K>()
      : new SailfishSerializer<K>();
  }

  @Override
  public Deserializer<K> getDeserializer(Class<K> c) {
    return isPigKeyClass(c)
      ? new PigSailfishDeserializer<K>(getConf(), c)
      : new SailfishDeserializer<K>(getConf(), c);
  }

}
