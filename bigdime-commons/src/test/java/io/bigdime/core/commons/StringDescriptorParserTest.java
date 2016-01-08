/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class StringDescriptorParserTest {

	@Test
	public void testOneInputWithOneTopicAndMultiplePartitions() {
		StringDescriptorParser descriptorParser = new StringDescriptorParser();
		Map<Object, String> descEnty = descriptorParser.parseDescriptor("unit-input", "topic1:par1,par2,par3");
		Assert.assertNotNull(descEnty);
		Assert.assertEquals(descEnty.size(), 3);
		Assert.assertEquals(descEnty.get("topic1:par1"), "unit-input");
		Assert.assertEquals(descEnty.get("topic1:par2"), "unit-input");
		Assert.assertEquals(descEnty.get("topic1:par3"), "unit-input");
	}

	@Test
	public void testOneInputWithOneTopicAndOnePartition() {
		StringDescriptorParser descriptorParser = new StringDescriptorParser();
		Map<Object, String> descEnty = descriptorParser.parseDescriptor("unit-input", "topic1:par1");
		Assert.assertNotNull(descEnty);
		Assert.assertEquals(descEnty.size(), 1);
		Assert.assertEquals(descEnty.get("topic1:par1"), "unit-input");
	}

	@Test
	public void testOneInputWithOneTopicAndNoPartition() {
		StringDescriptorParser descriptorParser = new StringDescriptorParser();
		Map<Object, String> descEnty = descriptorParser.parseDescriptor("unit-input", "topic1:");
		Assert.assertNotNull(descEnty);
		Assert.assertEquals(descEnty.size(), 1);
		Assert.assertEquals(descEnty.get("topic1"), "unit-input");
	}
}
