package com.sherston.s3;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;

/**
 * Helps to overcome the shortcommings of the java.net.URL class that has
 * troubles to handle multiple protocols.
 * 
 * Decorates the URL class allowing to use a custom protocol
 * 
 * @author pejot
 * 
 */
public class S3Url {
	public static final String PROTOCOL = "s3";
	public static final String DEFAULT_DELIMITER = "/";
	
	private String path;
	private String bucketName;

	public S3Url(String input) throws MalformedURLException {
		this.prepare(input);
	}

	/**
	 * hacks the url to the right size
	 * 
	 * @param url
	 * @throws MalformedURLException
	 */
	private void prepare(String url) throws MalformedURLException {
		if (url == null) {
			throw new IllegalArgumentException("S3URL needs to be non zero");
		}

		if (!url.substring(0, 5).equals(PROTOCOL + "://")) {
			throw new IllegalArgumentException(
					"The protol that you used needs to be s3. eg s3://... you supplied: "
							+ url);
		}
		
		url = url.substring(5);
		this.path = "";
		
		if(url.contains("/")){
			String[] elements = url.split(DEFAULT_DELIMITER);			
			List elementsList = new ArrayList<String>(Arrays.asList(elements));
			this.bucketName = (String) elementsList.remove(0);
			
			//still something left
			if(elementsList.size() > 0){
				Joiner joiner = Joiner.on(DEFAULT_DELIMITER).skipNulls();
				this.path = joiner.join(elementsList);
			}
		}else{
			this.bucketName = url;	
			this.path = "";
		}
		
	}

	public String getPath() {		
		return this.path;
	}

	public String getProtocol() {
		return PROTOCOL;
	}

	public String getBucketName() {
		return this.bucketName;
	}
}
