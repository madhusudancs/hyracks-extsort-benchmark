package edu.uci.ics.hyracks.extsort_benchmark;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.FrameFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;


public class Driver {

    private static final Log LOG = LogFactory.getLog(Driver.class);
    private static String LIB = "lib";
    private boolean profiling;

    private String applicationName = "benchmark";
    private String HYRACKS_HOME = "benchmark";
    private IHyracksClientConnection hcc;

    private int frameLimit = 1000;
    private final int tableSize = 10485767;

    public void runJob(String inputFileName, String outputFileName, int frameLimit, String ipAddress,
            int port, boolean profiling) throws HyracksException {
        LOG.info("job started");
        long start = System.currentTimeMillis();
        long time = 0;

        this.profiling = profiling;
        try {
            if (hcc == null)
                hcc = new HyracksConnection(ipAddress, port);

            URLClassLoader classLoader = (URLClassLoader) this.getClass().getClassLoader();
            URL[] urls = classLoader.getURLs();
            String jarFile = "";
            for (URL url : urls)
                if (url.toString().endsWith(".jar"))
                    jarFile = url.getPath();

            start = System.currentTimeMillis();
            installApplication(jarFile);

            FileSplit[] inputFileSplits = ClusterConfig.getFileSplits(inputFileName);
            FileSplit[] outputFileSplits = ClusterConfig.getFileSplits(outputFileName);
            String[] constraints = ClusterConfig.getLocationConstraints(inputFileSplits);

            JobSpecification job = new JobSpecification();

            IFileSplitProvider ordersSplitProvider = new ConstantFileSplitProvider(inputFileSplits);
            RecordDescriptor lineItemDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                    UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

            RecordDescriptor groupDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                    UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

            FileScanOperatorDescriptor lineItemScanner = new FileScanOperatorDescriptor(job, ordersSplitProvider,
                    new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE }, ','), lineItemDesc);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, lineItemScanner, constraints);

            int[] keyFields = new int[] { 0 };
            IBinaryComparatorFactory[] comparators = new IBinaryComparatorFactory[] {
                    PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY)};
            IBinaryHashFunctionFactory[] hashFunctions = new IBinaryHashFunctionFactory[] {
                    PointableBinaryHashFunctionFactory.of(UTF8StringPointable.FACTORY)};
            INormalizedKeyComputerFactory nmk = new UTF8StringNormalizedKeyComputerFactory();
            IAggregatorDescriptorFactory agg = new MultiFieldsAggregatorFactory(
                    new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) });
            IAggregatorDescriptorFactory merge = new MultiFieldsAggregatorFactory(
                    new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) });

            ExternalGroupOperatorDescriptor grouper = new ExternalGroupOperatorDescriptor(job, keyFields, frameLimit,
                    comparators, nmk, agg, merge, groupDesc, new HashSpillableTableFactory(
                            new FieldHashPartitionComputerFactory(keyFields, hashFunctions), tableSize), false);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, grouper, constraints);

            ExternalGroupOperatorDescriptor grouper2 = new ExternalGroupOperatorDescriptor(job, keyFields, frameLimit,
                    comparators, nmk, merge, merge, groupDesc, new HashSpillableTableFactory(
                            new FieldHashPartitionComputerFactory(keyFields, hashFunctions), tableSize), false);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, grouper2, constraints);

            IFileSplitProvider outSplits = new ConstantFileSplitProvider(outputFileSplits);
            IOperatorDescriptor printer = new FrameFileWriterOperatorDescriptor(job, outSplits);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(job, printer, constraints);

            ITuplePartitionComputerFactory partitionFactory = new FieldHashPartitionComputerFactory(keyFields,
                    hashFunctions);
            job.connect(new OneToOneConnectorDescriptor(job), lineItemScanner, 0, grouper, 0);
            job.connect(new MToNPartitioningConnectorDescriptor(job, partitionFactory), grouper, 0, grouper2, 0);
            job.connect(new OneToOneConnectorDescriptor(job), grouper2, 0, printer, 0);

            execute(job);

            destroyApplication(applicationName);
            long end = System.currentTimeMillis();
            time = end - start;
            LOG.info("job finished in " + time + "ms");
            LOG.info("job finished");
        } catch (Exception e) {
            throw new HyracksException(e);
        }
    }

    private void execute(JobSpecification job) throws Exception {
        JobId jobId = hcc.createJob(applicationName, job,
                profiling ? EnumSet.of(JobFlag.PROFILE_RUNTIME) : EnumSet.noneOf(JobFlag.class));
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
    }

    public void installApplication(String jarFile) throws Exception {
        String home = System.getProperty(HYRACKS_HOME);
        if (home == null)
            home = "./";
        String libDir = home + LIB;
        File dir = new File(libDir);
        if (!dir.isDirectory()) {
            throw new HyracksException(libDir + " is not a directory!");
        }
        System.out.println(dir.getAbsolutePath());
        File[] libJars = dir.listFiles(new FileFilter("jar"));
        Set<String> allJars = new TreeSet<String>();
        allJars.add(jarFile);
        for (File jar : libJars) {
            allJars.add(jar.getAbsolutePath());
        }
        File appZip = Utilities.getHyracksArchive(applicationName, allJars);
        System.out.println(applicationName + " " + appZip.getAbsolutePath());
        hcc.createApplication(applicationName, appZip);
    }

    public void destroyApplication(String jarFile) throws Exception {
        hcc.destroyApplication(applicationName);
    }

    private static class Options {
        @Option(name = "-inputfile", usage = "input file", required = true)
        public String inputPaths;

        @Option(name = "-outputfile", usage = "output file", required = true)
        public String outputPath;

        @Option(name = "-ip", usage = "ip address of cluster controller", required = true)
        public String ipAddress;

        @Option(name = "-port", usage = "port of cluster controller", required = true)
        public int port;

        @Option(name = "-framelimit", usage = "algorithm frame limit", required = true)
        public int frameLimit;

        @Option(name = "-runtime-profiling", usage = "whether to do runtime profifling", required = false)
        public String profiling = "false";
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        Driver driver = new Driver();
        driver.runJob(options.inputPaths, options.outputPath, options.frameLimit,
                options.ipAddress, options.port, Boolean.parseBoolean(options.profiling));
    }

}

class FileFilter implements FilenameFilter {
    private String ext;

    public FileFilter(String ext) {
        this.ext = "." + ext;
    }

    public boolean accept(File dir, String name) {
        return name.endsWith(ext);
    }
}
