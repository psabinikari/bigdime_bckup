/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * If the input descriptor is defined as text, then the text is parsed as follows:
 * @formatter:off
 * Case 1: If the src-desc is defined as:
 * "src-desc": {
 *   "input1" : "topic1:par1,par2,par3,"
 * }, then
 * a map with following entries will be returned.
 * {topic1:par1=input1, topic1:par2=input1, topic1:par3=input1}
 * 
 * Case 3: If the src-desc is defined as:
 * "src-desc": {
 *   "input1" : "topic1:"
 * },
 * a map with following entries will be returned.
 * {topic1:par1=input1}
 * @author Neeraj Jain
 *
 */
public class StringDescriptorParser implements DescriptorParser {

	public Map<Object, String> parseDescriptor(String inputKey, Object descriptorObj) {
		Map<Object, String> descInputEntry = new HashMap<>();
		String descriptors = descriptorObj.toString();
		String[] inputArray = descriptors.split(DataConstants.COLON);

		if (inputArray.length == 2) {
			descInputEntry = parseOneToTwo( inputKey, inputArray);
		} else if (inputArray.length == 1) {
			descInputEntry = parseOneToOne( inputKey, inputArray);
		} else {//dont parse, send it as it is
			descInputEntry.put(descriptors, inputKey);
		}
		return descInputEntry;
	}
	
	private Map<Object, String> parseOneToTwo(String inputKey, String[] inputArray) {
		Map<Object, String> descInputEntry = new HashMap<>();
		final String part1 = inputArray[0].trim();
		final String part2Str = inputArray[1];
		final String[] part2Array = part2Str.split(DataConstants.COMMA);
		for (String part2 : part2Array) {
			part2 = part2.trim();
			String desc = part1 + DataConstants.COLON + part2;
			descInputEntry.put(desc, inputKey);
		}
		return descInputEntry;
	}
	private Map<Object, String> parseOneToOne(String inputKey, String[] inputArray) {
		Map<Object, String> descInputEntry = new HashMap<>();
		final String part2Str = inputArray[0];
		final String[] part2Array = part2Str.split(DataConstants.COMMA);
		for (final String part2 : part2Array) {
			String desc = part2.trim();
			if (StringUtils.isBlank(desc))
				throw new IllegalArgumentException("invalid value(blank) specified for " + inputKey);
			descInputEntry.put(desc, inputKey.trim());
		}
		return descInputEntry;
	}
}
