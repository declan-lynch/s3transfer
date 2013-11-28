package com.sherston.s3.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
 * Due to the problem, that s3fs has a weird problem creating folder structures,
 * sometimes it might by that ther folders are visible in the AWS console but
 * invisible in s3fs mounted on disk
 * 
 * To fix this problem, one needs to create a folder using mkdir on the mounted
 * share where the last folder was. This is done by reading the API and figuring
 * out the folders, than creating them.
 * 
 * @author pejot
 * 
 */
public class S3FSFolderFixerCommand implements Command {
	public static final String ENABLED = "s3fix";
	public static final String DEST_FOLDER = "dest-dir";

	private S3Url sourceUrl;
	private Path destPath;
	private S3Service s3;

	@Override
	public OptionGroup getCommandOptions() {
		OptionBuilder.hasArg(false);
		OptionBuilder.withLongOpt(ENABLED);
		OptionBuilder
				.withDescription("Uses the D3Copy command, copying the files from source to destination.");
		Option enabled = OptionBuilder.create();

		OptionBuilder.hasArg(true);
		OptionBuilder.withLongOpt(DEST_FOLDER);
		OptionBuilder
				.withDescription("(s3fix only) This is the directory where the problematic s3fs share is mounted.");
		Option destDir = OptionBuilder.create();

		OptionGroup gr = new OptionGroup();
		gr.addOption(enabled).addOption(destDir);

		return gr;
	}

	@Override
	public String getEnabledOptionName() {
		return ENABLED;
	}

	@Override
	public void runCommand(S3Service s3, BoundedExecutor ex, CommandLine cli,
			OutputStream log) {
		this.s3 = s3;
		String sourceUrlString = cli.getOptionValue(S3Tool.SOURCE_URL);
		Path destDirString = Paths.get(cli.getOptionValue(DEST_FOLDER));

		try {
			destPath = destDirString.toRealPath();
			System.out.println("path: " + destDirString);
			System.out.println("realpath: " + destPath);
		} catch (IOException x) {
			throw new RuntimeException("The destionation path '"
					+ destDirString + "' doesn't exist");
		}
		Writer out = new PrintWriter(log);

		try {
			sourceUrl = new S3Url(sourceUrlString);
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
					out.write("Creating folders...\r\n");
					lastChunkKey = sendForExecution(chunk);
					totalDone += chunk.getObjects().length;
					out.write("Total processed: " + totalDone + "\r\n");
					out.flush();
				}

			} catch (Exception e) {
				System.out.println("abort, abort!!");
				System.out.println(e.getMessage());
				e.printStackTrace();
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

		String separator = System.getProperty("file.separator");

		for (StorageObject obj : chunk.getObjects()) {
			Path p = Paths.get(obj.getKey().replace(sourceUrl.getPath(), ""));

			if (p.startsWith(separator)) {
				p = Paths.get(p.toString().substring(1, p.toString().length()));
			}
			p = Paths.get(this.destPath + separator + p.toString());			

			try {
				Files.createDirectories(p.getParent());
			} catch (IOException e) {
				System.out.println("Problem creating structure: " + p.getParent());
				e.printStackTrace();
			}

			lastObject = obj.getKey();
		}

		return lastObject;
	}

}
