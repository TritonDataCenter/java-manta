package com.joyent.manta.client;

import com.joyent.manta.config.MapConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public class MantaMBeanSupervisorTest {

    @Test
    public void canExposeConfig() throws Exception {
        final MantaMBeanSupervisor supervisor = new MantaMBeanSupervisor();
        final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

        supervisor.expose(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beans = supervisor.getBeans();
        Assert.assertEquals(beans.size(), 1);

        final List<String> propList = Arrays.asList(MapConfigContext.ALL_PROPERTIES);

        final ObjectName configName = extractSingleBeanName(beans);

        Assert.assertTrue(beanServer.isRegistered(configName));
        for (MBeanAttributeInfo attrInfo : beanServer.getMBeanInfo(configName).getAttributes()) {
            propList.contains(attrInfo.getName());
        }

        supervisor.close();
        Assert.assertFalse(beanServer.isRegistered(configName));
    }

    @Test
    public void canReuseObjectNamesAfterReset() throws Exception {
        final MantaMBeanSupervisor supervisor = new MantaMBeanSupervisor();
        final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

        supervisor.expose(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beans = supervisor.getBeans();
        Assert.assertEquals(beans.size(), 1);

        final ObjectName configName = extractSingleBeanName(beans);

        Assert.assertTrue(beanServer.isRegistered(configName));
        supervisor.reset();
        Assert.assertFalse(beanServer.isRegistered(configName));

        supervisor.expose(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beansAfterReset = supervisor.getBeans();
        Assert.assertEquals(beansAfterReset.size(), 1);

        final ObjectName newConfigName = extractSingleBeanName(beansAfterReset);

        Assert.assertEquals(configName, newConfigName);

        supervisor.close();
        Assert.assertFalse(beanServer.isRegistered(configName));
    }

    // TEST UTILITY METHODS

    private ObjectName extractSingleBeanName(final Map<ObjectName, DynamicMBean> beans) {
        final ObjectName[] objectNames = beans.keySet().toArray(new ObjectName[0]);
        Assert.assertEquals(objectNames.length, 1);

        return objectNames[0];
    }
}
