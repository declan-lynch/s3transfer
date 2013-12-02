package com.sherston.s3.command;

import java.io.OutputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionGroup;
import org.jets3t.service.S3Service;

import com.sherston.executors.BoundedExecutor;

/**
 * Interface for all commands
 * 
 * @author pejot
 * 
 */
public interface Command {

	OptionGroup getCommandOptions();

	String getEnabledOptionName();

	void runCommand(S3Service s3, BoundedExecutor ex, CommandLine cli, OutputStream log);
}
