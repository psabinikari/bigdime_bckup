/**
 * Copyright (C) 2015 Stubhub.
 */
package io.bigdime.core.commons;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;

/**
 * Helper class for various String related operations.F
 * 
 * @author Neeraj Jain
 *
 */
@Component
public final class StringHelper {
	private static StringHelper instance = new StringHelper();

	private StringHelper() {
	}

	public static StringHelper getInstance() {
		return instance;
	}

	/**
	 * Checks if the propertyName is a string, and return it's value from
	 * properties if it exists there. If the propertyName is not a String,
	 * propertyName.toString() is returned.
	 * 
	 * @see #redeemToken(String, Properties)
	 * 
	 * @param propertyName
	 *            tokenized property name
	 * @param properties
	 *            look for the property name in this object and return the value
	 * @return value of the property specified by propertyName
	 */
	public String redeemToken(final Object propertyName, final Properties properties) {
		Preconditions.checkNotNull(propertyName, "propertyName must be not null");
		Preconditions.checkNotNull(properties, "properties must be not null");
		if (propertyName instanceof String) {
			return redeemToken(propertyName.toString(), properties);
		}
		return propertyName.toString();
	}

	/**
	 * De-tokenizes propertyName by removing "${" from start and "}" from end
	 * and then look for it in the properties object. If the properties object
	 * contains the property, property's value is returned otherwise
	 * propertyName is returned as it is. If the propertyName is "${host}" and
	 * property contains a property with name "host" and value as "localhost",
	 * "localhost" is returned otherwise "${host}" is returned.
	 * 
	 * @param propertyName
	 *            tokenized property name
	 * @param properties
	 *            look for the property name in this object and return the value
	 * @return value of the property specified by propertyName
	 */
	public String redeemToken(final String propertyName, final Properties properties) {
		Preconditions.checkNotNull(propertyName, "propertyName must be not null");
		Preconditions.checkNotNull(properties, "properties must be not null");
		String propValue = propertyName.toString();
		Pattern p = Pattern.compile("\\$\\{([\\w\\-_\\.]+)\\}+");
		Matcher m = p.matcher(propValue);
		while (m.find()) {
			String tokenizedString = m.group(1);// e.g.
												// headerName=account
			String newValue = properties.getProperty(tokenizedString);
			if (newValue != null) {
				return StringUtils.trim(newValue);
			}
		}
		return propertyName;
	}

	/**
	 * Parses data by converting it to String and then looking for new line
	 * character from the end.
	 * 
	 * 
	 * <p>
	 * <ul>
	 * <li>null, if no new line character found
	 * <li>a two element array of byte array
	 * <ul>
	 * <li>first element contains the byte[], that ends with a new line char,
	 * inclusive.
	 * <li>second element contains
	 * <ul>
	 * <li>the left over data if there is any after the last new line character
	 * <li>or, an empty string if the data ends with a new line character
	 * 
	 * 
	 * @param data
	 *            not null byte array which needs to be partitioned
	 * @return byte[][] or throws an {@link IllegalArgumentException} if data is
	 *         null
	 */
	public static byte[][] partitionByNewLine(final byte[] data) {
		Preconditions.checkNotNull(data);
		String str = new String(data, Charset.defaultCharset());
		int lastNewLineCharIndex = str.lastIndexOf("\n");
		if (lastNewLineCharIndex == -1) {
			return null;
		} else {
			byte[][] lines = new byte[2][];
			lines[0] = Arrays.copyOf(data, lastNewLineCharIndex + 1);
			lines[1] = Arrays.copyOfRange(data, lastNewLineCharIndex + 1, str.length());
			return lines;
		}
	}

	/**
	 * 
	 * If the secondPart has some data in it, assumption firstPart must be not
	 * null and must end with a line. If the secondPart contains some complete
	 * lines and a partial line, append the complete lines to firstPart. Return
	 * the array with first element containing complete lines and second element
	 * containing a partial line.
	 * 
	 * 
	 * 
	 * @param firstPart
	 * @param secondPart
	 * @return
	 */
	@Deprecated
	public static byte[][] appendAndPartitionByNewLine(final byte[] firstPart, final byte[] secondPart) {
		byte[][] partitionedArray = partitionByNewLine(secondPart);

		if (partitionedArray != null) {
			partitionedArray[0] = ArrayUtils.addAll(firstPart, partitionedArray[0]);
			return partitionedArray;
		} else {
			if (new String(firstPart).isEmpty())
				return null;
			partitionedArray = new byte[2][];
			partitionedArray[0] = firstPart;
			partitionedArray[1] = secondPart;
			return partitionedArray;// appendAndPartitionByNewLine("".getBytes(),
									// firstPart);
		}
	}

	public static String getRelativePath(String absolutePath, String basePath) {
		if (StringUtils.isBlank(absolutePath))
			throw new IllegalArgumentException("absolutePath must be a non empty value");
		if (StringUtils.isBlank(basePath))
			return absolutePath;
		return absolutePath.substring(basePath.length());
	}
}
