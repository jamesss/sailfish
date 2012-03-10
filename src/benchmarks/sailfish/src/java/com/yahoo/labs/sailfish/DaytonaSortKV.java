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

package com.yahoo.labs.sailfish;

import org.apache.hadoop.io.Text;

public class DaytonaSortKV implements BenchmarkKV {

  static class RandomGenerator {
    private long seed = 0;
    private static final long mask32 = (1l<<32) - 1;
    /**
     * The number of iterations separating the precomputed seeds.
     */
    private static final int seedSkip = 128 * 1024 * 1024;
    /**
     * The precomputed seed values after every seedSkip iterations.
     * There should be enough values so that a 2**32 iterations are
     * covered.
     */
    private static final long[] seeds = new long[]{0L,
                                                   4160749568L,
                                                   4026531840L,
                                                   3892314112L,
                                                   3758096384L,
                                                   3623878656L,
                                                   3489660928L,
                                                   3355443200L,
                                                   3221225472L,
                                                   3087007744L,
                                                   2952790016L,
                                                   2818572288L,
                                                   2684354560L,
                                                   2550136832L,
                                                   2415919104L,
                                                   2281701376L,
                                                   2147483648L,
                                                   2013265920L,
                                                   1879048192L,
                                                   1744830464L,
                                                   1610612736L,
                                                   1476395008L,
                                                   1342177280L,
                                                   1207959552L,
                                                   1073741824L,
                                                   939524096L,
                                                   805306368L,
                                                   671088640L,
                                                   536870912L,
                                                   402653184L,
                                                   268435456L,
                                                   134217728L,
                                                  };

    /**
     * Start the random number generator on the given iteration.
     * @param initalIteration the iteration number to start on
     */
    RandomGenerator(long initalIteration) {
      int baseIndex = (int) ((initalIteration & mask32) / seedSkip);
      seed = seeds[baseIndex];
      for(int i=0; i < initalIteration % seedSkip; ++i) {
        next();
      }
    }

    RandomGenerator() {
      this(0);
    }

    long next() {
      seed = (seed * 3141592621l + 663896637) & mask32;
      return seed;
    }
  }

  private Text value = new Text();
  private byte[] data;
  private RandomGenerator rand;
  private byte[] keyBytes = new byte[12];
  private byte[] spaces = "          ".getBytes();
  private byte[][] filler = new byte[26][];
  {
    for(int i=0; i < 26; ++i) {
      filler[i] = new byte[10];
      for(int j=0; j<10; ++j) {
        filler[i][j] = (byte) ('A' + i);
      }
    }
  }



  /**
   * Add the rowid to the row.
   * @param rowId
   */
  private void addRowId(long rowId) {
    byte[] rowid = Integer.toString((int) rowId).getBytes();
    int padSpace = 10 - rowid.length;
    if (padSpace > 0) {
      value.append(spaces, 0, 10 - rowid.length);
    }
    value.append(rowid, 0, Math.min(rowid.length, 10));
  }

  /**
   * Add the required filler bytes. Each row consists of 7 blocks of
   * 10 characters and 1 block of 8 characters.
   * @param rowId the current row number
   */
  private void addFiller(long rowId) {
    int base = (int) ((rowId * 8) % 26);
    for(int i=0; i<7; ++i) {
      value.append(filler[(base+i) % 26], 0, 10);
    }
    value.append(filler[(base+7) % 26], 0, 8);
  }

  public DaytonaSortKV(long seed, int maxValueLength) {
    rand = new RandomGenerator(3 * seed);
    data = new byte[maxValueLength];
  }
  @Override
  public byte[] getData() {
    return data;
  }
  /**
   * Add a random key to the text
   * @param rowId
   */
  public void generateKey(int keyLen) {
    for(int i=0; i<3; i++) {
      long temp = rand.next() / 52;
      keyBytes[3 + 4*i] = (byte) (' ' + (temp % 95));
      temp /= 95;
      keyBytes[2 + 4*i] = (byte) (' ' + (temp % 95));
      temp /= 95;
      keyBytes[1 + 4*i] = (byte) (' ' + (temp % 95));
      temp /= 95;
      keyBytes[4*i] = (byte) (' ' + (temp % 95));
    }
    System.arraycopy(keyBytes, 0, data, 0, keyLen);
  }

  @Override
  public void generateValue(long rowId, int valueLen) {
    value.clear();
    addRowId(rowId);
    addFiller(rowId);
    byte[] valBytes = value.getBytes();
    int pos = 0;
    for (pos = 0; pos < Math.min(valueLen, valBytes.length); pos++)
      data[pos] = valBytes[pos];
    int base = (int) ((rowId * 8) % 26);
    for (; pos < valueLen; pos++)
      data[pos] = filler[(base + pos) % 26][0];

  }

}
