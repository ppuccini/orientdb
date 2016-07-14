/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.index.lsmtree;

import com.orientechnologies.common.types.OModifiableBoolean;

/**
 * @author Sergey Sitnikov
 */
public interface OTree<K, V> {

  long size();

  boolean contains(K key);

  V get(K key);

  V get(K key, OModifiableBoolean found);

  boolean put(K key, V value);

  boolean remove(K key);

  K beginningKey();

  K endKey();

  OKeyValueCursor<K, V> range(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end, OCursor.Direction direction);

  OKeyCursor<K> keyRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end, OCursor.Direction direction);

  OValueCursor<V> valueRange(K beginningKey, K endKey, OCursor.Beginning beginning, OCursor.End end, OCursor.Direction direction);

}
