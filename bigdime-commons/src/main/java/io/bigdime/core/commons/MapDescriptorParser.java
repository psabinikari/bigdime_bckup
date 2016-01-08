/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.HashMap;
import java.util.Map;

/**
 * If the input descriptor is defined as a JsonNode, then the node is converted
 * to a Map.
 * 
 * { "input1" : { "entity-name" : "entityNameValue", "topic" : "topic1",
 * "partition" : "part1" } "input2" : "some-value" }
 * 
 * {{entity-name=entityNameValue, topic=topic1, partition=part1}, input1}
 * {some-value, input2}
 * 
 * 
 * @author Neeraj Jain
 *
 */
public class MapDescriptorParser implements DescriptorParser {

	public Map<Object, String> parseDescriptor(String inputKey, Object descriptors) {
		Map<Object, String> descInputEntry = new HashMap<>();
		descInputEntry.put(descriptors, inputKey);
		return descInputEntry;
	}
}
