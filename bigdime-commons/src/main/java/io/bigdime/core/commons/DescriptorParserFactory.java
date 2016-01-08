/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.util.Map;

/**
 *
 * @author Neeraj Jain
 *
 */
public class DescriptorParserFactory {

	public static DescriptorParser getDescriptorParser(Object descriptor) {
		if (descriptor instanceof String) {
			return new StringDescriptorParser();
		} else if (descriptor instanceof Map) {
			return new MapDescriptorParser();
		} else {
			throw new IllegalArgumentException(descriptor.getClass() + " not supported");
		}
	}
}
