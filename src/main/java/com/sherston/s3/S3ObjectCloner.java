package com.sherston.s3;

import org.jets3t.service.model.S3Object;

/**
 * Utility to clone the S3Object between buckets and directories
 * @author pejot
 *
 */
public class S3ObjectCloner {

	public static final String DEFAULT_DELIMITER = "/";
	
	/**
	 * 
	 * @param inputObj
	 * @param sourcePrefix
	 * @param destingationPrefix
	 * @param delimiter
	 * @return
	 */
	static public S3Object getClone(S3Object inputObj, String sourcePrefix,
			String destingationPrefix, String delimiter) {
		S3Object destObj = (S3Object) inputObj.clone();
		String destinationKey = inputObj.getKey();

		if (delimiter == null) {
			delimiter = DEFAULT_DELIMITER;
		}

		// copy from dir to dir
		if (sourcePrefix.length() > 0 && destingationPrefix.length() > 0) {
			destinationKey = destinationKey.replace(sourcePrefix,
					destingationPrefix);
		}

		// copy from main bucket "direcory" to some different dir
		if ((sourcePrefix.length() == 0 || sourcePrefix.equals(delimiter))
				&& destingationPrefix.length() > 0) {
			destinationKey = destingationPrefix + delimiter + inputObj.getKey();
		}

		destObj.setKey(destinationKey);

		return destObj;
	}

}
