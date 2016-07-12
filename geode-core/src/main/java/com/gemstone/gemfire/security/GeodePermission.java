/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.security;

import org.apache.shiro.authz.permission.WildcardPermission;

public class GeodePermission extends WildcardPermission {

  public static String ALL_REGIONS = "*";
  public static String ALL_KEYS = "*";

  public enum Resource {
    NULL,
    CLUSTER,
    DATA
  }

  public enum Operation {
    NULL,
    MANAGE,
    WRITE,
    READ
  }

  public Resource getResource() {
    return resource;
  }

  public Operation getOperation() {
    return operation;
  }

  public String getRegionName() {
    return regionName;
  }

  public String getKey() {
    return key;
  }

  // these default values are used when creating a lock around an operation
  private Resource resource = Resource.NULL;
  private Operation operation = Operation.NULL;
  private String regionName = ALL_REGIONS;
  private String key = ALL_KEYS;

  public GeodePermission() {
    this(Resource.NULL, Operation.NULL);
  }

  public GeodePermission(String resource, String operation) {
    this(resource, operation, ALL_REGIONS);
  }

  public GeodePermission(String resource, String operation, String regionName) {
    this(resource, operation, regionName, ALL_KEYS);
  }

  public GeodePermission(String resource, String operation, String regionName, String key) {
    this((resource==null) ? Resource.NULL : Resource.valueOf(resource),
      (operation == null) ? Operation.NULL : Operation.valueOf(operation),
      regionName,
      key);
  }

  public GeodePermission(Resource resource, Operation operation){
    this(resource, operation, ALL_REGIONS);
  }

  public GeodePermission(Resource resource, Operation operation, String regionName){
    this(resource, operation, regionName, ALL_KEYS);
  }

  public GeodePermission(Resource resource, Operation operation, String regionName, String key){
    if(resource != null) this.resource = resource;
    if(operation != null) this.operation = operation;
    if(regionName != null) this.regionName = regionName;
    if(key != null) this.key = key;

    setParts(this.resource+":"+this.operation+":"+this.regionName+":"+this.key, true);
  }

  @Override
  public String toString() {
    if (ALL_REGIONS.equals(regionName)) {
      return getResource() + ":" + getOperation();
    } else if(ALL_KEYS.equals(key)) {
      return resource + ":" + operation + ":" + regionName;
    }
    else{
      return resource + ":" + operation + ":" + regionName + ":" + key;
    }
  }

}
