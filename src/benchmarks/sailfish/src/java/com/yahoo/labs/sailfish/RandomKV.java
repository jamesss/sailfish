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

import java.util.Random;

/**
 * Class that generates random key/value pairs. The data is not very compressible.
 */
public class RandomKV implements BenchmarkKV {

  byte[] data;
  Random randomGenerator;
  private static final char[] symbols = new char[79];
  // keys are 0..9,'a'..'z'
  private static final int keySymbols = 36;

  static {
    for (int idx = 0; idx < 10; ++idx)
      symbols[idx] = (char) ('0' + idx);
    for (int idx = 10; idx < 36; ++idx)
      symbols[idx] = (char) ('a' + idx - 10);
    for (int idx = 37; idx < 63; ++idx)
      symbols[idx] = (char) ('A' + idx - 37);
    // 32 is ' '; 47 is '/'.  take all
    for (int idx = 64; idx < 79; ++idx)
      symbols[idx] = (char) (32 + idx - 64);
  }

  public RandomKV(long seed, int maxValueLength) {
    randomGenerator = new Random(seed);
    data = new byte[maxValueLength];
  }

  public byte[] getData() {
    return data;
  }


  @Override
  public void generateKey(int keyLen) {
    for (int i = 0; i < keyLen; i++) {
      data[i] = (byte) symbols[randomGenerator.nextInt(keySymbols)];
    }
  }

  @Override
  public void generateValue(long rowId, int valueLen) {
    for (int i = 0; i < valueLen; i++) {
      data[i] = (byte) symbols[randomGenerator.nextInt(symbols.length)];
    }
  }

}
