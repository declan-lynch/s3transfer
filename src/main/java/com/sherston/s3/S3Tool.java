package com.sherston.s3;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.jets3t.service.S3Service;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

import com.sherston.executors.BoundedExecutor;
import com.sherston.s3.command.Command;
import com.sherston.s3.command.NullCommand;

/**
 * S3Tool
 * 
 * @author pejot
 * 
 */
public class S3Tool {

	public final static String AWS_ACCESS_KEY_ID = "awskey";
	public final static String AWS_SECRET_ACCESS_KEY = "awssecret";
	public final static String OPT_THREADS = "threads";
	public final static String SOURCE_URL = "source-url";
	public final static String DESTINATION_URL = "dest-url";
	public final static String CHUNKSIZE = "chunksize";
	public final static int DEFAULT_CHUNKSIZE = 100;
	public final static int DEFAULT_THREADS = 40;

	private Map<String, Command> commandMap = new HashMap<>();
	private CommandLine commands;
	private Options options;

	public static void main(String[] args) {
		S3Tool s3 = new S3Tool(args);
	}

	/**
	 * Takes the straight CLI arguments as an array of strings
	 * 
	 * @param cmdArgs
	 */
	public S3Tool(String[] cmdArgs) {
		try {
			this.options = this.constructGnuOptions();
			this.initCommands();
			this.commands = this.useGnuParser(cmdArgs, options);

			for (Option c : commands.getOptions()) {
				System.out.println(c.getLongOpt() + " : " + c.getValue());
			}

			if (commands.hasOption("help")) {
				printUsage("S3Tool", options);
			} else {
				this.runCommand();				
			}
		} catch (ParseException pe) {
			System.out
					.println("Problem with the arguments: " + pe.getMessage());

			printUsage("S3Tool", options);
		} catch (Exception e) {
			System.out.println(e.toString());
		}

	}

	/**
	 * Registers commands
	 * 
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private void initCommands() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {
		this.registerCommandClass("com.sherston.s3.command.NullCommand");
		this.registerCommandClass("com.sherston.s3.command.S3CopyMover");
		this.registerCommandClass("com.sherston.s3.command.S3FSFolderFixerCommand");
	}

	/**
	 * Prepares and returns a S3Service instance
	 * 
	 * @param commands
	 * @return
	 */
	private S3Service getS3Service(CommandLine commands) {
		String accessKey = (String) commands.getOptionValue(AWS_ACCESS_KEY_ID);
		String secretKey = (String) commands
				.getOptionValue(AWS_SECRET_ACCESS_KEY);

		AWSCredentials awsCredentials = new AWSCredentials(accessKey, secretKey);

		try {
			return new RestS3Service(awsCredentials);
		} catch (Exception e) {
			System.out
					.println("Had problems creating the S3 service. Sure the credentials are correct?");
			System.out.println(e.toString());
		}

		return null;
	}

	public BoundedExecutor getBoundedExecutor(CommandLine commands) {
		int threadNum = (int) Integer.parseInt(commands.getOptionValue(
				OPT_THREADS, String.valueOf(DEFAULT_THREADS)));

		ExecutorService e = Executors.newFixedThreadPool(threadNum);
		BoundedExecutor be = new BoundedExecutor(e, threadNum);

		return be;
	}

	/**
	 * Registers a class name with the tool
	 * 
	 * builds a map of enabled option name -> object
	 * 
	 * @param classname
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public void registerCommandClass(String classname)
			throws ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		Class<?> comm = (Class<?>) Class.forName(classname);
		Command cObj = (Command) comm.newInstance();
		options.addOptionGroup(cObj.getCommandOptions());
		this.commandMap.put(cObj.getEnabledOptionName(), cObj);
	}

	/**
	 * Runs a command specified by the commandline arguments
	 * 
	 * 
	 */
	public void runCommand() {
		Command command = getCommandEnabled(commands);

		S3Service s3 = getS3Service(commands);
		BoundedExecutor be = getBoundedExecutor(commands);
		command.runCommand(s3, be, commands, System.out);		
		be.shutdown();
	}

	/**
	 * Performs a check if nore than one commadn is enabled (configuration
	 * problem)
	 * 
	 * @param cmd
	 * @return
	 */
	private Command getCommandEnabled(CommandLine cmd) {
		Command enabled = null;

		Iterator<Entry<String, Command>> it = commandMap.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Command> c = it.next();
			if (cmd.hasOption(c.getKey())) {
				if (null != enabled) {
					throw new InvalidParameterException(
							"More than one command used at a time.");
				} else {
					enabled = c.getValue();
					break;
				}
			}
		}

		if (null == enabled) {
			throw new InvalidParameterException(
					"There needs to be a command used");
		}

		return enabled;
	}

	/**
	 * Apply Apache Commons CLI GnuParser to command-line arguments.
	 * 
	 * @param commandLineArguments
	 *            Command-line arguments to be processed with Gnu-style parser.
	 * @throws ParseException
	 * @see http
	 *      ://marxsoftware.blogspot.co.uk/2008/11/command-line-parsing-with-
	 *      apache.html
	 */
	public CommandLine useGnuParser(final String[] commandLineArguments,
			Options gnuOptions) throws ParseException {
		final CommandLineParser cmdLineGnuParser = new GnuParser();
		CommandLine commandLine = null;
		commandLine = cmdLineGnuParser.parse(gnuOptions, commandLineArguments);

		return commandLine;
	}

	/**
	 * Construct and provide GNU-compatible Options.
	 * 
	 * @return Options expected from command-line of GNU form.
	 */
	private Options constructGnuOptions() {

		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired();
		OptionBuilder.withLongOpt(AWS_ACCESS_KEY_ID);
		OptionBuilder.isRequired();
		OptionBuilder.withArgName(AWS_ACCESS_KEY_ID);
		OptionBuilder.withDescription("(Required) AWS access key.");

		final Option accessKey = OptionBuilder.create();

		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired();
		OptionBuilder.withLongOpt(AWS_SECRET_ACCESS_KEY);
		OptionBuilder.withArgName(AWS_SECRET_ACCESS_KEY);
		OptionBuilder.withDescription("(Required) AWS secred token.");
		final Option secretKey = OptionBuilder.create();

		OptionBuilder.hasArg(true);
		OptionBuilder.isRequired();
		OptionBuilder.withLongOpt(SOURCE_URL);
		OptionBuilder.withArgName(SOURCE_URL);
		OptionBuilder
				.withDescription("(Required) The s3 url of the source e.g.: s3://bucket/folder/folder");
		final Option sourceUrl = OptionBuilder.create();

		OptionBuilder.hasArg(true);
		OptionBuilder.withLongOpt(DESTINATION_URL);
		OptionBuilder.withArgName(DESTINATION_URL);
		OptionBuilder
				.withDescription("(Required) The s3 url of the source e.g.: s3://bucket/folder/folder");
		final Option destUrl = OptionBuilder.create();

		OptionBuilder.hasArg(true);
		OptionBuilder
				.withDescription("(Optional) How many threads will there be for execution. Default is "
						+ DEFAULT_THREADS);
		OptionBuilder.withLongOpt(OPT_THREADS);
		final Option threads = OptionBuilder.create("t");

		OptionBuilder.hasArg(false);
		OptionBuilder.withLongOpt("help");
		OptionBuilder.withDescription("Shows this help.");
		final Option help = OptionBuilder.create("h");

		OptionBuilder.hasArg(true);
		OptionBuilder.withLongOpt(CHUNKSIZE);
		OptionBuilder.withDescription("How much to get from s3 at a time? Default 100, max 1000.");
		final Option chunksize = OptionBuilder.create();

		final Options opt = new Options();
		opt.addOption(accessKey).addOption(secretKey).addOption(sourceUrl)
				.addOption(destUrl).addOption(chunksize).addOption(help)
				.addOption(threads);

		return opt;
	}

	/**
	 * Print usage information to provided OutputStream.
	 * 
	 * @param applicationName
	 *            Name of application to list in usage.
	 * @param options
	 *            Command-line options to be part of usage.
	 * @param out
	 *            OutputStream to which to write the usage information.
	 */
	public static void printUsage(final String applicationName,
			final Options options) {
		PrintWriter writer = new PrintWriter(System.out);
		HelpFormatter usageFormatter = new HelpFormatter();
		usageFormatter.printHelp(80, "cmd syntax", "HELP:", options, "");
	}
}
