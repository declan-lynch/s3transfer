package com.sherston.s3.command;

import java.io.OutputStream;
import java.net.MalformedURLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.jets3t.service.S3Service;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.model.StorageObject;

import com.sherston.executors.BoundedExecutor;
import com.sherston.s3.S3Tool;
import com.sherston.s3.S3Url;

public class CounterCommand implements Command {
	private S3Url sourceUrl;
	public static final String OPT_ENABLED = "counter";
	private S3Service s3;

	public OptionGroup getCommandOptions() {
		Option enabled = OptionBuilder
				.hasArg(false)
				.withLongOpt(OPT_ENABLED)
				.withDescription(
						"Selects the counter command. This command just counts the object in the bucket.")
				.create();

		OptionGroup group = new OptionGroup();
		group.addOption(enabled);

		return group;
	}

	@Override
	public String getEnabledOptionName() {
		return OPT_ENABLED;
	}

	@Override
	public void runCommand(S3Service s3, BoundedExecutor ex, CommandLine cli,
			OutputStream log) {
		this.s3 = s3;
		String sourceUrlString = cli.getOptionValue(S3Tool.SOURCE_URL);

		try {
			this.sourceUrl = new S3Url(sourceUrlString);

		} catch (MalformedURLException e) {
			throw new RuntimeException("There is a paramater problem: "
					+ e.getMessage());
		}

		int chunkSize = (int) Integer.parseInt(cli.getOptionValue(
				S3Tool.CHUNKSIZE, String.valueOf(S3Tool.DEFAULT_CHUNKSIZE)));

		System.out.println("Starting Null command loop... ");

		long startTime = System.currentTimeMillis();

		// getting the current data in chunks and sending the chunks for
		// execution
		String lastChunkKey = null;
		long totalCount = 0;
		long curTime;
		boolean keepGoing = true;

		while (keepGoing) {
			try {

				StorageObjectsChunk chunk = s3.listObjectsChunked(
						sourceUrl.getBucketName(), sourceUrl.getPath(), null,
						chunkSize, lastChunkKey);

				totalCount += chunkSize;

				if (chunk.getObjects().length == 0) {
					// nothing left to read
					keepGoing = false;
				} else {
					totalCount += chunk.getObjects().length;
				}

				curTime = System.currentTimeMillis();

				long elapsedMillis = curTime - startTime;
				double perSec = ((double) ((double) totalCount / (double) elapsedMillis)) * 1000;

				String info = "Getting from " + sourceUrlString
						+ " in chunks: " + chunkSize + " at " + perSec;
				System.out.println("");
				System.out.println(info);

			} catch (ServiceException e) {
				System.out.println(e.getMessage());
			}

		}

	}
}
