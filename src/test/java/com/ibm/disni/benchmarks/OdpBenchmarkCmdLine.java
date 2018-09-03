package com.ibm.disni.benchmarks;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

public class OdpBenchmarkCmdLine extends RdmaBenchmarkCmdLine {
  private static final String DEVICE_NUM_KEY = "d";
  private static final String USE_PREFETCH_KEY = "prefetch";

  public boolean getDoPrefetch() {
    return doPrefetch;
  }

  private boolean doPrefetch = false;

  public int getDeviceNum() {
    return deviceNum;
  }

  public long getFileSize() {
    return fileSize;
  }

  private int deviceNum = 0;

  private static final String FILE_SIZE_KEY = "f";
  private long fileSize = 15*1024*1024*1024L; //15G


  public OdpBenchmarkCmdLine(String appName) {
    super(appName);

    addOption(Option.builder(DEVICE_NUM_KEY).desc("device number to print ODP stats")
      .hasArg().type(Number.class).build());

    addOption(Option.builder(FILE_SIZE_KEY).desc("File size to mmap")
      .hasArg().type(Number.class).build());

    addOption(Option.builder(USE_PREFETCH_KEY).desc("Whether to use prefetch")
      .hasArg().type(Number.class).build());
  }


  @Override
  protected void getOptionsValue(CommandLine line) throws ParseException {
    super.getOptionsValue(line);

    if (line.hasOption(DEVICE_NUM_KEY)) {
      deviceNum = ((Number)line.getParsedOptionValue(DEVICE_NUM_KEY)).intValue();
    }

    if (line.hasOption(FILE_SIZE_KEY)) {
      fileSize = ((Number)line.getParsedOptionValue(FILE_SIZE_KEY)).longValue();
    }

    if (line.hasOption(USE_PREFETCH_KEY)) {
      doPrefetch = ((Number)line.getParsedOptionValue(USE_PREFETCH_KEY)).intValue() == 1;
    }
  }
}
