package com.sherston.s3.command;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

import com.sherston.executors.BoundedExecutor;
import com.sherston.s3.S3ObjectCloner;
import com.sherston.s3.S3Tool;
import com.sherston.s3.S3Url;

/**
 * Handles the moving of files between buckets with an AWS COPY command
 * 
 * @author pejot
 * 
 */
public class S3CopyMover implements Command {
	public static final String ENABLED = "copy";
	public static final String KEY_COPY = "copybykey";
	private S3Url sourceUrl;
	private S3Url destUrl;
	private BoundedExecutor executor;
	private S3Service s3;
	boolean keyCheckOnly;

	@Override
	public OptionGroup getCommandOptions() {
		OptionBuilder.hasArg(false);
		OptionBuilder.withLongOpt(ENABLED);
		OptionBuilder
				.withDescription("Uses the D3Copy command, copying the files from source to destination.");
		Option enabled = OptionBuilder.create();

		OptionBuilder.hasArg(false);
		OptionBuilder.withLongOpt(KEY_COPY);
		OptionBuilder
				.withDescription("Copies files only if the key doesn't exist. Withoug this enabled it will check if the files are different and overwrite.");
		Option key = OptionBuilder.create();

		OptionGroup gr = new OptionGroup();
		gr.addOption(enabled).addOption(key);

		return gr;
	}

	@Override
	public String getEnabledOptionName() {
		return ENABLED;
	}

	@Override
	public void runCommand(S3Service s3, BoundedExecutor ex, CommandLine cli,
			OutputStream log) {
		executor = ex;
		this.s3 = s3;
		String sourceUrlString = cli.getOptionValue(S3Tool.SOURCE_URL);
		String destUrlString = cli.getOptionValue(S3Tool.DESTINATION_URL);

		keyCheckOnly = cli.hasOption(KEY_COPY);
		System.out.println("key: " + keyCheckOnly);

		Writer out = new PrintWriter(log);

		try {
			sourceUrl = new S3Url(sourceUrlString);
			destUrl = new S3Url(destUrlString);
		} catch (MalformedURLException e) {
			throw new RuntimeException("There is a paramater problem: "
					+ e.getMessage());
		}

		int chunkSize = (int) Integer.parseInt(cli.getOptionValue(
				S3Tool.CHUNKSIZE, String.valueOf(S3Tool.DEFAULT_CHUNKSIZE)));

		// getting the current data in chunks and sending the chunks for
		// execution
		String lastChunkKey = null;
		boolean keepGoing = true;
		long totalDone = 0;
		while (keepGoing) {
			try {
				out.write("Getting " + chunkSize + " from " + sourceUrlString
						+ " ...\r\n");
				out.flush();

				StorageObjectsChunk chunk = s3.listObjectsChunked(
						sourceUrl.getBucketName(), sourceUrl.getPath(), null,
						chunkSize, lastChunkKey);

				if (chunk == null || chunk.getObjects().length == 0) {
					// nothing left to read
					keepGoing = false;
					break;
				} else {
					out.write("Distributing tasks to move...\r\n");
					sendForExecution(chunk);
					totalDone += chunk.getObjects().length;
					out.write("Total done: " + totalDone + "\r\n");
					out.flush();
					lastChunkKey = chunk.getObjects()[chunk.getObjects().length - 1]
							.getKey();
				}

			} catch (IOException | ServiceException e) {
				System.out.println("Problem, Captain!");
				System.out.println(e.getMessage());
			}

		}

	}

	/**
	 * Sends all the objects for execution returns the last object
	 * 
	 * @param chunk
	 * @return
	 */
	private String sendForExecution(StorageObjectsChunk chunk) {
		String lastObject = null;
		for (StorageObject obj : chunk.getObjects()) {
			if (obj.getKey().endsWith("/")) {
				continue;
			}
			lastObject = obj.getKey();

			StorageObject destObject = S3ObjectCloner.getClone((S3Object) obj,
					sourceUrl.getPath(), destUrl.getPath(), null);

			try {
				executor.submitTask(new Move(this.s3, obj.getBucketName(),
						destUrl.getBucketName(), obj, destObject, keyCheckOnly));
			} catch (RejectedExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// handle
			}
		}

		return lastObject;
	}

	class Move implements Runnable {

		private S3Service s3;
		private String sourceBucket;
		private String destBucket;
		private StorageObject sourceObject;
		private StorageObject destObject;
		private boolean checkKeyOnly;

		public Move(S3Service s3, String sourceBucket, String destBucket,
				StorageObject sourceObject, StorageObject destObject,
				boolean checkKeyOnly) {
			super();
			this.s3 = s3;
			this.sourceBucket = sourceBucket;
			this.destBucket = destBucket;
			this.sourceObject = sourceObject;
			this.destObject = destObject;
			this.checkKeyOnly = checkKeyOnly;
		}

		@Override
		public void run() {
			if (!checkKeyOnly) {
				if (sameDestinationObjectAlreadyExists()) {
					System.out.print("s1");
					return;
				}
			} else {
				boolean destEtag = this.destinationKeyExists();
				if (false != destEtag) {
					System.out.print("s2");
					return;
				}
			}

			try {
				Map<String, Object> moveResult = s3.copyObject(
						this.sourceBucket, sourceObject.getKey(),
						this.destBucket, this.destObject, false);
				System.out.print("c");
			} catch (Exception e) {
				System.out.print("e");
			}
		}

		private boolean destinationKeyExists() {
			try {
				StorageObject destObjectdetail = s3.getObjectDetails(
						this.destBucket, destObject.getKey());

				return null != destObjectdetail.getETag();
			} catch (Exception e) {

			}
			return false;
		}

		/**
		 * Checks the object detais if it already may exist
		 * 
		 * @return
		 */
		public boolean sameDestinationObjectAlreadyExists() {
			try {
				StorageObject destObjectdetail = s3.getObjectDetails(
						this.destBucket, this.destObject.getKey());

				System.out.println("dest etag: " + destObjectdetail.getETag());
				System.out.println("source etag: " + sourceObject.getETag());

				return destObjectdetail.getETag()
						.equals(sourceObject.getETag());
			} catch (Exception e) {

			}
			return false;
		}

	}
}
