package com.gemstone.gemfire.test.dunit;

import static com.gemstone.gemfire.test.dunit.DUnitEnv.getAllDistributedSystemProperties;
import static com.gemstone.gemfire.test.dunit.Invoke.invokeInEveryVM;
import static com.gemstone.gemfire.test.dunit.Invoke.invokeInLocator;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache30.GlobalLockingDUnitTest;
import com.gemstone.gemfire.cache30.MultiVMRegionTestCase;
import com.gemstone.gemfire.cache30.RegionTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.CreationStackGenerator;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.admin.ClientStatsManager;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.DataSerializerPropogationDUnitTest;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.GemFireTracer;

@SuppressWarnings("serial")
public class DUnitTestRule implements TestRule, Serializable {
  private static final Logger logger = LogService.getLogger();

  private static final String LOG_PER_TEST_CLASS_PROPERTY = "dunitLogPerTest";

  private volatile String className;
  private volatile String methodName;
  
  private static class StaticContext {
    private static volatile boolean logPerTestClass;
    private static volatile boolean logPerTestMethod;

    private static volatile InternalDistributedSystem system;
    private static volatile String previousSystemCreatedInTestClass;
    private static volatile Properties previousProperties;
    
    private static volatile String testClassName;
    private static volatile String testMethodName;
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  protected DUnitTestRule(final Builder builder) {
    StaticContext.logPerTestClass = builder.logPerTestClass;
    StaticContext.logPerTestMethod = builder.logPerTestMethod;
  }
  
  public DUnitTestRule() {
    StaticContext.logPerTestClass = Boolean.getBoolean(LOG_PER_TEST_CLASS_PROPERTY);
  }
  
  @Override
  public Statement apply(final Statement base, final Description description) {
    starting(description);
    return statement(base);
  }
  
  /**
   * Invoked when a test is about to start
   */
  protected void starting(final Description description) {
    this.className = description.getClassName();
    this.methodName = description.getMethodName();
    
    boolean newTestClass = false;
    if (StaticContext.testClassName != null && !this.className.equals(StaticContext.testClassName)) {
      // new test class detected
      newTestClass = true;
    }
    
    if (newTestClass && StaticContext.logPerTestClass) {
      disconnectAllFromDS();
    }
    
    StaticContext.testClassName = this.className;
    StaticContext.testMethodName = this.methodName;
  }
  
  protected void before() throws Throwable {
    DUnitLauncher.launchIfNeeded();
    
    setUpDistributedTestCase();
  }

  protected void after() throws Throwable {
    tearDownDistributedTestCase();
  }
  
  public String getClassName() {
    return this.className;
  }
  
  public String getMethodName() {
    return this.methodName;
  }
  
  public static String getUniqueName() {
    return StaticContext.testClassName + "_" + StaticContext.testMethodName;
  }
  
  public static String getTestClassName() {
    return StaticContext.testClassName;
  }
  
  public static String getTestMethodName() {
    return StaticContext.testMethodName;
  }
  
  private Statement statement(final Statement base) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          base.evaluate();
        } finally {
          after();
        }
      }
    };
  }
  
  //---------------------------------------------------------------------------
  // customization methods
  //---------------------------------------------------------------------------
  
  /**
   * Returns a <code>Properties</code> object used to configure a
   * connection to a {@link
   * com.gemstone.gemfire.distributed.DistributedSystem}.
   * Unless overridden, this method will return an empty
   * <code>Properties</code> object.
   *
   * @since 3.0
   */
  public static Properties getDistributedSystemProperties() {
    // TODO: extension point
    return new Properties();
  }

  /**
   * Tears down the test. This method is called by the final {@link #tearDown()} method and should be overridden to
   * perform actual test cleanup and release resources used by the test.  The tasks executed by this method are
   * performed before the DUnit test framework using Hydra cleans up the client VMs.
   * <p/>
   * @throws Exception if the tear down process and test cleanup fails.
   * @see #tearDown
   * @see #tearDownAfter()
   */
  protected void tearDownBefore() throws Exception {
    // TODO: extension point
  }

  /**
   * Tears down the test.  Performs additional tear down tasks after the DUnit tests framework using Hydra cleans up
   * the client VMs.  This method is called by the final {@link #tearDown()} method and should be overridden to perform
   * post tear down activities.
   * <p/>
   * @throws Exception if the test tear down process fails.
   * @see #tearDown()
   * @see #tearDownBefore()
   */
  protected void tearDownAfter() throws Exception {
    // TODO: extension point
  }

  //---------------------------------------------------------------------------
  // public getSystem methods
  //---------------------------------------------------------------------------
  
  /**
   * Returns this VM's connection to the distributed system.  If
   * necessary, the connection will be lazily created using the given
   * <code>Properties</code>.  Note that this method uses hydra's
   * configuration to determine the location of log files, etc.
   * Note: "final" was removed so that WANTestBase can override this method.
   * This was part of the xd offheap merge.
   *
   * @see hydra.DistributedConnectionMgr#connect
   * @since 3.0
   */
  public static InternalDistributedSystem getSystem(final Properties properties) {
    if (!hasPreviousSystem()) {
      final Properties newProperties = getAllDistributedSystemProperties(properties);
      
      if (StaticContext.logPerTestMethod) {
        newProperties.put(DistributionConfig.LOG_FILE_NAME, getUniqueName() + ".log");
        newProperties.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, getUniqueName() + ".gfs");
      } else if (StaticContext.logPerTestClass) {
        newProperties.put(DistributionConfig.LOG_FILE_NAME, getTestClassName() + ".log");
        newProperties.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, getTestClassName() + ".gfs");
      }
      
      StaticContext.system = (InternalDistributedSystem)DistributedSystem.connect(newProperties);

      StaticContext.previousSystemCreatedInTestClass = getTestClassName();
      StaticContext.previousProperties = newProperties;
      
    } else {
      boolean needNewSystem = previousPropertiesAreDifferentThan(properties);
        
      needNewSystem = needNewSystem || propertiesAreDifferentThanCurrentSystem(properties);
      
      if (needNewSystem) {
        // the current system does not meet our needs to disconnect and
        // call recursively to get a new system.
        logger.info("Disconnecting from current DS in order to make a new one");
        disconnectFromDS();
        getSystem(properties);
      }
    }
    return StaticContext.system;
  }
  
  private static boolean hasPreviousSystem() {
    if (StaticContext.system == null) {
      StaticContext.system = InternalDistributedSystem.getAnyInstance();
    }
    
    return StaticContext.system != null && StaticContext.system.isConnected();
  }
  
  private static boolean previousPropertiesAreDifferentThan(final Properties properties) {
    boolean needNewSystem = false;
    if (!getTestClassName().equals(StaticContext.previousSystemCreatedInTestClass)) {
      // previous system was created in a previous test class
      final Properties newProperties = getAllDistributedSystemProperties(properties);
      needNewSystem = !newProperties.equals(StaticContext.previousProperties);
      if (needNewSystem) {
        logger.info(
            "Test class has changed and the new DS properties are not an exact match. "
                + "Forcing DS disconnect. Old props = "
                + StaticContext.previousProperties + "new props=" + newProperties);
      }
    }
    return needNewSystem;
  }
  
  private static boolean propertiesAreDifferentThanCurrentSystem(final Properties properties) {
    // previous system was created in this test class
    final Properties currentProperties = StaticContext.system.getProperties();
    for (Iterator<Map.Entry<Object,Object>> iter = properties.entrySet().iterator(); iter.hasNext(); ) {
      final Map.Entry<Object,Object> entry = iter.next();
      final String key = (String) entry.getKey();
      final String value = (String) entry.getValue();
      if (!value.equals(currentProperties.getProperty(key))) {
        logger.info("Forcing DS disconnect. For property " + key
                            + " old value = " + currentProperties.getProperty(key)
                            + " new value = " + value);
        return true;
      }
    }
    return false;
  }
  
  /**
   * Returns this VM's connection to the distributed system.  If
   * necessary, the connection will be lazily created using the
   * <code>Properties</code> returned by {@link
   * #getDistributedSystemProperties}.
   *
   * @see #getSystem(Properties)
   *
   * @since 3.0
   */
  public static InternalDistributedSystem getSystem() {
    return getSystem(getDistributedSystemProperties());
  }

  /**
   * Returns a loner distributed system that isn't connected to other
   * vms
   * 
   * @since 6.5
   */
  public static InternalDistributedSystem getLonerSystem() {
    Properties props = getDistributedSystemProperties();
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    return getSystem(props);
  }
  
  /**
   * Returns a loner distributed system in combination with enforceUniqueHost
   * and redundancyZone properties.
   * Added specifically to test scenario of defect #47181.
   */
  public static InternalDistributedSystem getLonerSystemWithEnforceUniqueHost() {
    Properties props = getDistributedSystemProperties();
    props.put(DistributionConfig.MCAST_PORT_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    props.put(DistributionConfig.ENFORCE_UNIQUE_HOST_NAME, "true");
    props.put(DistributionConfig.REDUNDANCY_ZONE_NAME, "zone1");
    return getSystem(props);
  }

  /**
   * Returns an mcast distributed system that is connected to other
   * vms using a random mcast port.
   */
  public static InternalDistributedSystem getMcastSystem() {
    Properties props = getDistributedSystemProperties();
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    props.put(DistributionConfig.MCAST_PORT_NAME, ""+port);
    props.put(DistributionConfig.MCAST_TTL_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    return getSystem(props);
  }

  /**
   * Returns an mcast distributed system that is connected to other
   * vms using the given mcast port.
   */
  public static InternalDistributedSystem getMcastSystem(int jgroupsPort) {
    Properties props = getDistributedSystemProperties();
    props.put(DistributionConfig.MCAST_PORT_NAME, ""+jgroupsPort);
    props.put(DistributionConfig.MCAST_TTL_NAME, "0");
    props.put(DistributionConfig.LOCATORS_NAME, "");
    return getSystem(props);
  }

  /**
   * Returns whether or this VM is connected to a {@link
   * DistributedSystem}.
   */
  public static boolean isConnectedToDS() {
    return StaticContext.system != null && StaticContext.system.isConnected();
  }

  //---------------------------------------------------------------------------
  // public cleanup methods
  //---------------------------------------------------------------------------
  
  public static void cleanupAllVms() {
    cleanupThisVM();

    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        cleanupThisVM();
      }
    });
    
    invokeInLocator(new SerializableRunnable() {
      @Override
      public void run() {
        DistributionMessageObserver.setInstance(null);
        unregisterInstantiatorsInThisVM();
      }
    });
    
    DUnitLauncher.closeAndCheckForSuspects();
  }

  public static void unregisterAllDataSerializersFromAllVms() {
    unregisterDataSerializerInThisVM();
    
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        unregisterDataSerializerInThisVM();
      }
    });
    
    invokeInLocator(new SerializableRunnable() {
      @Override
      public void run() {
        unregisterDataSerializerInThisVM();
      }
    });
  }

  public static void unregisterInstantiatorsInThisVM() {
    // unregister all the instantiators
    InternalInstantiator.reinitialize();
    assertEquals(0, InternalInstantiator.getInstantiators().length);
  }
  
  public static void unregisterDataSerializerInThisVM() {
    DataSerializerPropogationDUnitTest.successfullyLoadedTestDataSerializer = false;
    // unregister all the Dataserializers
    InternalDataSerializer.reinitialize();
    // ensure that all are unregistered
    assertEquals(0, InternalDataSerializer.getSerializers().length);
  }

  public static void disconnectAllFromDS() {
    disconnectFromDS();
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() { 
        disconnectFromDS();
      }
    });
  }

  /**
   * Disconnects this VM from the distributed system
   */
  public static void disconnectFromDS() {
    GemFireCacheImpl.testCacheXml = null;
    
    if (StaticContext.system != null) {
      StaticContext.system.disconnect();
      StaticContext.system = null;
    }
    
    for (;;) {
      DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      if (ds == null) {
        break;
      }
      try {
        ds.disconnect();
      }
      catch (Exception e) {
        // ignore
      }
    }
    
    AdminDistributedSystemImpl ads = AdminDistributedSystemImpl.getConnectedInstance();
    if (ads != null) {
      ads.disconnect();
    }
  }

  //---------------------------------------------------------------------------
  // private static methods
  //---------------------------------------------------------------------------
  
  private static void setUpDistributedTestCase() throws Exception {
    setUpCreationStackGenerator();
    
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");

    final String vmid = System.getProperty("vmid");
    final String defaultDiskStoreName = "DiskStore-"  + vmid + "-"+ StaticContext.testClassName + "." + getTestMethodName();
    GemFireCacheImpl.setDefaultDiskStoreName(defaultDiskStoreName); // TODO: not thread safe
    
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        final String vmDefaultDiskStoreName = "DiskStore-" + h + "-" + v + "-" + StaticContext.testClassName + "." + StaticContext.testMethodName;
        setUpInVM(vm, StaticContext.testClassName, StaticContext.testMethodName, vmDefaultDiskStoreName);
      }
    }
    //System.out.println("\n\n[setup] START TEST " + getClass().getSimpleName()+"."+testName+"\n\n");
  }
  
  private static void setUpInVM(final VM vm, final String testClassNameToUse, final String testMethodNameToUse, final String diskStoreNameToUse) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        setUpCreationStackGenerator();
        StaticContext.testClassName = testClassNameToUse;
        StaticContext.testMethodName = testMethodNameToUse;
        System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");    
        GemFireCacheImpl.setDefaultDiskStoreName(diskStoreNameToUse); // TODO: not thread safe
      }
    });
  }
    
  /**
   * For logPerTest to work, we have to disconnect from the DS, but all
   * subclasses do not call super.tearDown(). To prevent this scenario
   * this method has been declared final. Subclasses must now override
   * {@link #tearDownBefore()} instead.
   * @throws Exception
   */
  private final void tearDownDistributedTestCase() throws Exception {
    tearDownBefore();
    realTearDown();
    tearDownAfter();
    
    tearDownCreationStackGenerator();

    tearDownInEveryVM();
  }

  private static void realTearDown() throws Exception {
    if (StaticContext.logPerTestMethod) {
      disconnectAllFromDS();
    }
    cleanupAllVms();
  }
    
  private static void tearDownInEveryVM() {
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {    
        tearDownCreationStackGenerator();
        StaticContext.testClassName = null;
        StaticContext.testMethodName = null;
      }
    });
  }
  
  private static void setUpCreationStackGenerator() {
    // the following is moved from InternalDistributedSystem to fix #51058
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(
    new CreationStackGenerator() {
      @Override
      public Throwable generateCreationStack(final DistributionConfig config) {
        final StringBuilder sb = new StringBuilder();
        final String[] validAttributeNames = config.getAttributeNames();
        for (int i = 0; i < validAttributeNames.length; i++) {
          final String attName = validAttributeNames[i];
          final Object actualAtt = config.getAttributeObject(attName);
          String actualAttStr = actualAtt.toString();
          sb.append("  ");
          sb.append(attName);
          sb.append("=\"");
          if (actualAtt.getClass().isArray()) {
            actualAttStr = InternalDistributedSystem.arrayToString(actualAtt);
          }
          sb.append(actualAttStr);
          sb.append("\"");
          sb.append("\n");
        }
        return new Throwable("Creating distributed system with the following configuration:\n" + sb.toString());
      }
    });
  }
  
  private static void tearDownCreationStackGenerator() {
    InternalDistributedSystem.TEST_CREATION_STACK_GENERATOR.set(InternalDistributedSystem.DEFAULT_CREATION_STACK_GENERATOR);
  }
  
  private static void cleanupThisVM() {
    IpAddress.resolve_dns = true;
    SocketCreator.resolve_dns = true;
    InitialImageOperation.slowImageProcessing = 0;
    DistributionMessageObserver.setInstance(null);
    QueryTestUtils.setCache(null);
    CacheServerTestUtil.clearCacheReference();
    RegionTestCase.preSnapshotRegion = null;
    GlobalLockingDUnitTest.region_testBug32356 = null;
    LogWrapper.close();
    ClientProxyMembershipID.system = null;
    MultiVMRegionTestCase.CCRegion = null;
    InternalBridgeMembership.unregisterAllListeners();
    ClientStatsManager.cleanupForTests();
    unregisterInstantiatorsInThisVM();
    GemFireTracer.DEBUG = Boolean.getBoolean("DistributionManager.DEBUG_JAVAGROUPS");
    Protocol.trace = GemFireTracer.DEBUG;
    DistributionMessageObserver.setInstance(null);
    QueryObserverHolder.reset();
    if (InternalDistributedSystem.systemAttemptingReconnect != null) {
      InternalDistributedSystem.systemAttemptingReconnect.stopReconnecting();
    }
    ExpectedExceptionString ex;
    while((ex = ExpectedExceptionString.poll()) != null) {
      ex.remove();
    }
  }

  /**
   * Builds an instance of DUnitTestRule
   * 
   * @author Kirk Lund
   */
  public static class Builder {
    private boolean logPerTestMethod;
    private boolean logPerTestClass;
    
    protected Builder() {}

    public Builder vmCount(final int vmCount) {
      return this;
    }
    
    /**
     * A new DistributedSystem with .log and .gfs will be created for each test method
     */
    public Builder logPerTestMethod(final boolean logPerTestMethod) {
      this.logPerTestMethod = logPerTestMethod;
      return this;
    }
    
    /**
     * A new DistributedSystem with .log and .gfs will be created for each test class
     */
    public Builder logPerTestClass(final boolean logPerTestClass) {
      this.logPerTestClass = logPerTestClass;
      return this;
    }
    
    public DUnitTestRule build() {
      return new DUnitTestRule(this);
    }
  }
}