package com.sherston.test.s3;

import static org.junit.Assert.*;

import java.net.MalformedURLException;

import com.sherston.s3.S3Url;

import org.junit.Test;

/**
 * Tests the s3url class
 * 
 * @author pejot
 * 
 */
public class S3UrlTest {

	/**
	 * Tests that the url's get interpreted by the class properly
	 */
	@Test
	public void testParsingUrls() {

		String url1 = "s3://bucket/folder";
		String url2 = "s3://bucket/";
		String url3 = "s3://bucket-2/folder/folder";
		String url4 = "s3://bucket-bucket-buclet/folderA/folderB";
		String url5 = "s3://bucket-bucket";

		S3Url s3Url1 = null;
		S3Url s3Url2 = null;
		S3Url s3Url3 = null;
		S3Url s3Url4 = null;
		S3Url s3Url5 = null;
		try {
			s3Url1 = new S3Url(url1);
			s3Url2 = new S3Url(url2);
			s3Url3 = new S3Url(url3);
			s3Url4 = new S3Url(url4);
			s3Url5 = new S3Url(url5);
		} catch (MalformedURLException e) {
			fail("url failed to parse: " + e.getMessage());
		}

		assertEquals("bucket", s3Url1.getBucketName());
		assertEquals("folder", s3Url1.getPath());
		assertEquals("s3", s3Url1.getProtocol());

		assertEquals("bucket", s3Url2.getBucketName());
		assertEquals("", s3Url2.getPath());
		
		assertEquals("bucket-2", s3Url3.getBucketName());
		assertEquals("folder/folder", s3Url3.getPath());
		
		assertEquals("bucket-bucket-buclet", s3Url4.getBucketName());
		assertEquals("folderA/folderB", s3Url4.getPath());
		
		assertEquals("bucket-bucket" , s3Url5.getBucketName());
		assertEquals("", s3Url5.getPath());
		
	}

}
