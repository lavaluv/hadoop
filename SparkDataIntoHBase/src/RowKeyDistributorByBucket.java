/**
 * Copyright 2010 Sematext International
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


import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides handy methods to distribute
 *
 * @author Alex Baranau
 */
public class RowKeyDistributorByBucket extends AbstractRowKeyDistributor {

  private int maxPrefix;

  /** Constructor reflection. DO NOT USE */
  public RowKeyDistributorByBucket() {
  }

  public RowKeyDistributorByBucket(int bucketsCount) {
    this.maxPrefix = bucketsCount;
  }

  @Override
  public byte[] getDistributedKey(byte[] originalKey) {
    byte[] key = Bytes.add(String.valueOf(
    		Long.parseLong(new String(originalKey)) % maxPrefix).getBytes(), originalKey);
 
    return key;
  }

  @Override
  public byte[] getOriginalKey(byte[] adjustedKey) {
    return Bytes.tail(adjustedKey, adjustedKey.length - 1);
  }

  @Override
  public byte[][] getAllDistributedKeys(byte[] originalKey) {
    return getAllDistributedKeys(originalKey, maxPrefix);
  }

  private static byte[][] getAllDistributedKeys(byte[] originalKey, int maxPrefix) {
	byte[][] keys = new byte[maxPrefix][];
    System.err.println("----------------getAllDistributedKeysERR");

    return keys;
  }

  @Override
  public String getParamsToStore() {
    return String.valueOf(maxPrefix);
  }

  @Override
  public void init(String params) {
    maxPrefix = Integer.parseInt(params);
  }
}
