package com.ibm.disni.benchmarks;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

public class SendRecvCmdLine extends RdmaBenchmarkCmdLine {
	private int queueDepth;
	private final static String QUEUEDEPTH_KEY = "q";
	private final static int QUEUEDEPTH_DEFAULT = 1;

	private final static String MODE_KEY = "m";
	public enum Mode {
		RDMA, TCP
	}
	private Mode mode = Mode.RDMA;

	public SendRecvCmdLine(String appName) {
		super(appName);

		addOption(Option.builder(QUEUEDEPTH_KEY).desc("queue depth")
				.hasArg().type(Number.class).build());
		addOption(Option.builder(MODE_KEY).desc("benchmark mode rdma|tcp")
			.hasArg().type(String.class).build());
	}

	@Override
	protected void getOptionsValue(CommandLine line) throws ParseException {
		super.getOptionsValue(line);

		if (line.hasOption(QUEUEDEPTH_KEY)) {
			queueDepth = ((Number)line.getParsedOptionValue(QUEUEDEPTH_KEY)).intValue();
		} else {
			queueDepth = QUEUEDEPTH_DEFAULT;
		}

		if (line.hasOption(MODE_KEY)) {
			mode = Mode.valueOf(((String)line.getParsedOptionValue(MODE_KEY)).toUpperCase());
		}
	}

	public int getQueueDepth() {
		return queueDepth;
	}

	public Mode getMode() {
		return mode;
	}
}
