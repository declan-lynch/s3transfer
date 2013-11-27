package com.sherston.s3;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.acl.AccessControlList;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;
import org.jets3t.service.security.AWSCredentials;

import com.sherston.s3.command.Command;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

public class Main {

	private static String AWS_ACCESS_KEY_ID;
	private static String AWS_SECRET_ACCESS_KEY;
	private static String SOURCE_BUCKET_NAME;
	private static String DEST_BUCKET_NAME;
	private static Integer CHUNKSIZE;
	private static Integer LOOPEXIT;
	private static String PREFIXSOURCE;
	private static String PREFIXDEST;
	// private static final String DEST_BUCKET_NAME = "karimderrick";

	private static RestS3Service restService;
	private static S3Bucket sourceBucket;

	public static void main(String[] args) {
		
//		AWS_ACCESS_KEY_ID = args[0];
//		AWS_SECRET_ACCESS_KEY = args[1];
//		SOURCE_BUCKET_NAME = args[2];
//		DEST_BUCKET_NAME = args[3];
//		CHUNKSIZE = Integer.parseInt(args[4]);
//		LOOPEXIT = Integer.parseInt(args[5]);
//
//		AWSCredentials awsCredentials = new AWSCredentials(AWS_ACCESS_KEY_ID,
//				AWS_SECRET_ACCESS_KEY);
//
//		final String delimiter = "/";
//
//		String[] sourcebits = SOURCE_BUCKET_NAME.split(delimiter, 2);
//		SOURCE_BUCKET_NAME = sourcebits[0];
//		PREFIXSOURCE = sourcebits.length == 2 ? sourcebits[1] : "";
//
//		String[] destbits = DEST_BUCKET_NAME.split(delimiter, 2);
//		DEST_BUCKET_NAME = destbits[0];
//		PREFIXDEST = destbits.length == 2 ? destbits[1] : "";
//
//		try {
//			restService = new RestS3Service(awsCredentials);
//			sourceBucket = restService.getBucket(SOURCE_BUCKET_NAME);
//		} catch (Exception e) {
//			System.out.println(e.toString());
//		}
//
//		String[] prefixes = new String[] { "www/files" };
//
//		ExecutorService tPool = Executors.newFixedThreadPool(32);
//
//		long delay = 50;
//
//		for (String prefix : prefixes) {
//
//			StorageObject[] storageObjects;
//			String lastChunkKey = null;
//			Boolean moretoprocess = true;
//			Integer loopcount = 0;
//			while (moretoprocess) {
//
//				loopcount++;
//
//				System.out.println("Starting loop " + loopcount);
//
//				try {
//					StorageObjectsChunk chunks = restService
//							.listObjectsChunked(SOURCE_BUCKET_NAME,
//									PREFIXSOURCE, null, CHUNKSIZE, lastChunkKey);
//
//					if (chunks.getObjects().length > 0) {
//						moretoprocess = false;
//					}
//
//					storageObjects = chunks.getObjects();
//
//					if (storageObjects != null && storageObjects.length > 0) {
//						// ?
//					}
//
//					for (int i = 0; i < storageObjects.length; i++) {
//
//						if (null == storageObjects[i]
//								|| storageObjects[i].getKey().toString()
//										.endsWith("/")) {
//							continue;
//						}
//			
//						final S3Object sourceObject = (S3Object) storageObjects[i];
//						final String sourceObjectKey = sourceObject.getKey();
//						sourceObject
//								.setAcl(AccessControlList.REST_CANNED_PUBLIC_READ);
//						Command mover = new NullMover(restService,
//								sourceObject, sourceObjectKey);
//						
//						mover.checkMd5();
//						mover.setDestBucket(DEST_BUCKET_NAME);
//						mover.setSourceBucket(SOURCE_BUCKET_NAME);
//						mover.setSourcePrefix(PREFIXSOURCE);
//						mover.setDestPrefix(PREFIXDEST);
//						mover.init(delimiter);
//						while (true) {
//
//							try {
//
//								tPool.execute((Runnable) mover);
//								delay = 50;
//								break;
//
//							} catch (RejectedExecutionException r) {
//
//								System.out.println("Queue full: waiting "
//										+ delay + " ms");
//								try {
//									Thread.sleep(delay);
//								} catch (InterruptedException e) {
//									// TODO Auto-generated catch
//									// block
//									e.printStackTrace();
//								} // backoff and retry
//								delay += 50;
//
//							}
//
//						}
//						if (storageObjects.length - 1 == i) {
//							
//						}
//
//					}
//
//				} catch (S3ServiceException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				} catch (ServiceException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//
//				}
//				if (LOOPEXIT > 0 && loopcount >= LOOPEXIT) {
//					moretoprocess = false;
//				}
//			}
//
//		}
//
//		tPool.shutdown();
//
//		try {
//			tPool.awaitTermination(360000, TimeUnit.SECONDS);
//		} catch (InterruptedException e) {
//
//			e.printStackTrace();
//		}
//		System.out.println(" Completed!");

	}

	

}
