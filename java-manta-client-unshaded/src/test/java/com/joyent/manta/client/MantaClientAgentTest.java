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

public class MantaClientAgentTest {

    @Test
    public void canExposeConfig() throws Exception {
        final MantaClientAgent agent = new MantaClientAgent();
        final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

        agent.register(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beans = agent.getBeans();
        Assert.assertEquals(beans.size(), 1);

        final List<String> propList = Arrays.asList(MapConfigContext.ALL_PROPERTIES);

        final ObjectName configName = extractSingleBeanName(beans);

        Assert.assertTrue(beanServer.isRegistered(configName));
        for (MBeanAttributeInfo attrInfo : beanServer.getMBeanInfo(configName).getAttributes()) {
            propList.contains(attrInfo.getName());
        }

        agent.close();
        Assert.assertFalse(beanServer.isRegistered(configName));
    }

    @Test
    public void canReuseObjectNamesAfterReset() throws Exception {
        final MantaClientAgent agent = new MantaClientAgent();
        final MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();

        agent.register(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beans = agent.getBeans();
        Assert.assertEquals(beans.size(), 1);

        final ObjectName configName = extractSingleBeanName(beans);

        Assert.assertTrue(beanServer.isRegistered(configName));
        agent.reset();
        Assert.assertFalse(beanServer.isRegistered(configName));

        agent.register(new StandardConfigContext());

        final Map<ObjectName, DynamicMBean> beansAfterReset = agent.getBeans();
        Assert.assertEquals(beansAfterReset.size(), 1);

        final ObjectName newConfigName = extractSingleBeanName(beansAfterReset);

        Assert.assertEquals(configName, newConfigName);

        agent.close();
        Assert.assertFalse(beanServer.isRegistered(configName));
    }

    // TEST UTILITY METHODS

    private ObjectName extractSingleBeanName(final Map<ObjectName, DynamicMBean> beans) {
        final ObjectName[] objectNames = beans.keySet().toArray(new ObjectName[0]);
        Assert.assertEquals(objectNames.length, 1);

        return objectNames[0];
    }
}
