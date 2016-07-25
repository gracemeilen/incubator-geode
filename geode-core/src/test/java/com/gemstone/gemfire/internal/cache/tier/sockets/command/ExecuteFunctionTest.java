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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.Rule;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.powermock.api.mockito.PowerMockito;

import java.util.Properties;

import org.apache.tools.ant.taskdefs.Execute;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.internal.FunctionServiceManager;
import com.gemstone.gemfire.cache.operations.DestroyOperationContext;
import com.gemstone.gemfire.cache.operations.ExecuteFunctionOperationContext;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ChunkedMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.HandShake;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.SecurityService;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.cache.execute.Function;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.Test")
@PrepareForTest({ FunctionService.class })
public class ExecuteFunctionTest {

  private static final String FUNCTION = "function";
  private static final String FUNCTION_ID = "function_id";
  private static final boolean OPTIMIZE_FOR_WRITE = false;
  private static final Object CALLBACK_ARG = "arg";
  private static final byte[] RESULT = new byte[] {Integer.valueOf(1).byteValue()};

  @Mock
  private SecurityService securityService;
  @Mock
  private Message message;
  @Mock
  private ServerConnection serverConnection;
  @Mock
  private AuthorizeRequest authzRequest;
  @Mock
  private LocalRegion region;
  @Mock
  private GemFireCacheImpl cache;
  @Mock
  private InternalDistributedSystem distributedSystem;
  @Mock
  private InternalDistributedMember internalDistributedMember;
  @Mock
  private CacheServerStats cacheServerStats;
  @Mock
  private Message errorResponseMessage;
  @Mock
  private Message replyMessage;
  @Mock
  private Message responseMessage;
  @Mock
  private ChunkedMessage functionResponseMessage;
  @Mock
  private ChunkedMessage chunkedResponseMessage;
  @Mock
  private Part hasResultPart;
  @Mock
  private Part functionPart;
  @Mock
  private Part argsPart;
  @Mock
  private Part partPart;
  @Mock
  private Part callbackArgPart;
  @Mock
  private Function functionObject;
  @Mock
  private ExecuteFunctionOperationContext executeFunctionOperationContext;
  @Mock
  private AcceptorImpl acceptorImpl;
  @Mock
  private HandShake handShake;
  @Mock
  private InternalResourceManager resourceManager;

  @InjectMocks
  private ExecuteFunction executeFunction;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "statsDisabled", "true");

    this.executeFunction = new ExecuteFunction();
    MockitoAnnotations.initMocks(this);

    when(this.authzRequest.executeFunctionAuthorize(eq(FUNCTION_ID), eq(null), eq(null), eq(null), eq(OPTIMIZE_FOR_WRITE))).thenReturn(this.executeFunctionOperationContext);

    when(this.cache.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));
    when(this.cache.getDistributedSystem()).thenReturn(this.distributedSystem);
    when(this.cache.getResourceManager()).thenReturn(this.resourceManager);

    when(this.callbackArgPart.getObject()).thenReturn(CALLBACK_ARG);

    when(this.distributedSystem.getDistributedMember()).thenReturn(this.internalDistributedMember);

    when(this.functionObject.getId()).thenReturn(FUNCTION_ID);
    when(this.functionObject.optimizeForWrite()).thenReturn(OPTIMIZE_FOR_WRITE);

    when(this.functionPart.getStringOrObject()).thenReturn(FUNCTION);

    when(this.hasResultPart.getSerializedForm()).thenReturn(RESULT);

    when(this.internalDistributedMember.getId()).thenReturn("member1");

    when(this.message.getNumberOfParts()).thenReturn(4);
    when(this.message.getPart(eq(0))).thenReturn(this.hasResultPart);
    when(this.message.getPart(eq(1))).thenReturn(this.functionPart);
    when(this.message.getPart(eq(2))).thenReturn(this.argsPart);
    when(this.message.getPart(eq(3))).thenReturn(this.partPart);

    when(this.resourceManager.getHeapMonitor()).thenReturn(mock(HeapMemoryMonitor.class));

    when(this.serverConnection.getCache()).thenReturn(this.cache);
    when(this.serverConnection.getCacheServerStats()).thenReturn(this.cacheServerStats);
    when(this.serverConnection.getAuthzRequest()).thenReturn(this.authzRequest);
    when(this.serverConnection.getCachedRegionHelper()).thenReturn(mock(CachedRegionHelper.class));
    when(this.serverConnection.getReplyMessage()).thenReturn(this.replyMessage);
    when(this.serverConnection.getResponseMessage()).thenReturn(this.responseMessage);
    when(this.serverConnection.getErrorResponseMessage()).thenReturn(this.errorResponseMessage);
    when(this.serverConnection.getFunctionResponseMessage()).thenReturn(this.functionResponseMessage);
    when(this.serverConnection.getChunkedResponseMessage()).thenReturn(this.chunkedResponseMessage);
    when(this.serverConnection.getAcceptor()).thenReturn(this.acceptorImpl);
    when(this.serverConnection.getHandshake()).thenReturn(this.handShake);

    PowerMockito.mockStatic(FunctionService.class);
    when(FunctionService.getFunction(eq(FUNCTION))).thenReturn(functionObject);

//    PowerMockito.mockStatic(FunctionStats.class);
//    when(FunctionStats.getFunctionStats(any(), any())).thenReturn(mock(FunctionStats.class));
  }

  @Test
  public void nonSecureShouldSucceed() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(false);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.functionResponseMessage).sendChunk(this.serverConnection); // TODO: why do none of the reply message types get sent?
  }

  @Test
  public void withIntegratedSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeDataWrite();
    //verify(this.replyMessage).send(this.serverConnection); TODO: why do none of the reply message types get sent?
  }

  @Test
  public void withIntegratedSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(true);
    doThrow(new NotAuthorizedException("")).when(this.securityService).authorizeDataWrite();

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.securityService).authorizeDataWrite();
    verify(this.chunkedResponseMessage).sendChunk(this.serverConnection);
  }

  @Test
  public void withOldSecurityShouldSucceedIfAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.authzRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), eq(argsPart), eq(false));
    //verify(this.replyMessage).send(this.serverConnection); TODO: why do none of the reply message types get sent?
  }

  @Test
  public void withOldSecurityShouldThrowIfNotAuthorized() throws Exception {
    when(this.securityService.isClientSecurityRequired()).thenReturn(true);
    when(this.securityService.isIntegratedSecurity()).thenReturn(false);
    doThrow(new NotAuthorizedException("")).when(this.authzRequest).executeFunctionAuthorize(eq(FUNCTION_ID), any(), any(), eq(argsPart), eq(false));

    this.executeFunction.cmdExecute(this.message, this.serverConnection, 0);

    verify(this.chunkedResponseMessage).send(this.serverConnection);
  }

}
