/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MapDescriptorParserTest {

	@Test
	public void testOneInputWithOneTopicAndMultiplePartitions() throws JsonProcessingException, IOException {
		MapDescriptorParser descriptorParser = new MapDescriptorParser();

		String jsonString = "{\"unit-input\" : {\"entity-name\" : \"unit-entity-name-value\",\"topic\" : \"unit-topic-value\",\"partition\" : \"unit-partition-value\", \"unit-separator\" : \"unit-separator-value\"}}";

		ObjectMapper mapper = new ObjectMapper();
		JsonNode actualObj = mapper.readTree(jsonString);

		Map<String, Object> properties = new JsonHelper().getNodeTree(actualObj);

		Map<Object, String> descEnty = descriptorParser.parseDescriptor("unit-input", properties.get("unit-input"));

		Assert.assertNotNull(descEnty);
		Assert.assertEquals(descEnty.size(), 1);
		Object key = descEnty.keySet().iterator().next();
		Assert.assertEquals(descEnty.get(key), "unit-input");
		Assert.assertTrue(key instanceof Map);
		@SuppressWarnings("unchecked")
		Map<String, Object> inputDescriptorNodeValue = (Map<String, Object>) key;
		Assert.assertEquals(inputDescriptorNodeValue.get("entity-name"), "unit-entity-name-value");
		Assert.assertEquals(inputDescriptorNodeValue.get("topic"), "unit-topic-value");
		Assert.assertEquals(inputDescriptorNodeValue.get("partition"), "unit-partition-value");
		Assert.assertEquals(inputDescriptorNodeValue.get("unit-separator"), "unit-separator-value");
	}
}
