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

import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * User implementation of a authentication/authorization logic for Integrated Security.
 * The implementation will guard client/server, jmx, pulse, gfsh commands
 *
 * @since Geode 1.0
 */
public interface SecurityManager {

  /**
   * Initialize the SecurityManager. This is invoked when a cache is created
   *
   * @param securityProps
   *                the security properties obtained using a call to
   *                {@link DistributedSystem#getSecurityProperties}
   * @throws AuthenticationFailedException
   *                 if some exception occurs during the initialization
   */
  default void init(Properties securityProps) {}

  /**
   * Verify the credentials provided in the properties
   * @param props:
   *        it contains the security-username and security-password as keys of the properties
   * @return
   *        the authenticated Principal object
   * @throws AuthenticationFailedException
   */

  Principal authenticate(Properties props) throws AuthenticationFailedException;


  /**
   * Authorize the GeodePermission for a given Principal
   * @param principal
   *        The principal that's requesting the permission
   * @param permission
   *        The permission requested
   * @return
   *        true if authorized, false if not
   */
  default boolean authorize(Principal principal, GeodePermission permission) {
    return true;
  }

  /**
   * close any resources used by the SecurityManager, called when a cache is closed.
   */
  default void close() {}
}
