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

import java.io.ByteArrayOutputStream;

/**
 * Wrapper class that helps us get the underlying buffer in a ByteArrayOutputStream without copying.
 * @author sriramr
 *
 */
public class SailfishMapOutputBuffer extends ByteArrayOutputStream {
  public SailfishMapOutputBuffer() {
    super();
  }
  public SailfishMapOutputBuffer(int size) {
    super(size);
  }
  public byte[] getByteArray() {
    return buf;
  }
}
