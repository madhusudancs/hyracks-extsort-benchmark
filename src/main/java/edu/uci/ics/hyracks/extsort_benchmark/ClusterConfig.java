package edu.uci.ics.hyracks.extsort_benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class ClusterConfig {

    private static String[] NCs;
    private static String confPath = "conf/cluster";
    private static String propertiesPath = "conf/data.properties";
    private static Map<String, List<String>> ipToNcMapping;
    private static String[] stores;

    /**
     * let tests set config path to be whatever
     * 
     * @param confPath
     * @param propertiesPath
     */
    public static void setConfPath(String confPath, String propertiesPath) {
        ClusterConfig.confPath = confPath;
        ClusterConfig.propertiesPath = propertiesPath;
    }

    /**
     * get NC names running on one IP address
     * 
     * @param ipAddress
     * @return
     * @throws HyracksDataException
     */
    public static List<String> getNCNames(String ipAddress) throws HyracksException {
        if (NCs == null) {
            loadClusterConfig();
        }
        return ipToNcMapping.get(ipAddress);
    }

    /**
     * get file split provider
     * 
     * @param jobId
     * @return
     * @throws HyracksDataException
     */
    public static FileSplit[] getFileSplits(String fileName) throws HyracksException {
        if (stores == null) {
            loadStores();
        }
        if (NCs == null) {
            loadClusterConfig();
        }

        FileSplit[] fileSplits = new FileSplit[stores.length * NCs.length];
        int i = 0;
        for (String nc : NCs) {
            for (String st : stores) {
                FileSplit split = new FileSplit(nc, st + File.separator + fileName);
                fileSplits[i++] = split;
            }
        }
        return fileSplits;
    }

    private static void loadStores() throws HyracksException {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesPath));
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        String store = properties.getProperty("store");
        stores = store.split(";");
    }

    /**
     * get location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static String[] getLocationConstraints(FileSplit[] splits) throws HyracksException {
        if (stores == null) {
            loadStores();
        }
        if (NCs == null) {
            loadClusterConfig();
        }
        String[] locations = new String[splits.length];
        for (int i = 0; i < splits.length; i++) {
            locations[i] = splits[i].getNodeName();
        }
        return locations;
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setLocationConstraint(JobSpecification spec, IOperatorDescriptor operator,
            List<InputSplit> splits) throws HyracksException {
        if (stores == null) {
            loadStores();
        }
        if (NCs == null) {
            loadClusterConfig();
        }
        int count = splits.size();
        String[] locations = new String[splits.size()];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < splits.size(); i++) {
            try {
                String[] loc = splits.get(i).getLocations();
                Collections.shuffle(Arrays.asList(loc));
                if (loc.length > 0) {
                    InetAddress[] allIps = InetAddress.getAllByName(loc[0]);
                    for (InetAddress ip : allIps) {
                        if (ipToNcMapping.get(ip.getHostAddress()) != null) {
                            List<String> ncs = ipToNcMapping.get(ip.getHostAddress());
                            int pos = random.nextInt(ncs.size());
                            locations[i] = ncs.get(pos);
                        } else {
                            int pos = random.nextInt(NCs.length);
                            locations[i] = NCs[pos];
                        }
                    }
                } else {
                    int pos = random.nextInt(NCs.length);
                    locations[i] = NCs[pos];
                }
            } catch (IOException e) {
                throw new HyracksException(e);
            } catch (InterruptedException e) {
                throw new HyracksException(e);
            }
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, locations);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, count);
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setLocationConstraint(JobSpecification spec, IOperatorDescriptor operator)
            throws HyracksException {
        if (stores == null) {
            loadStores();
        }
        if (NCs == null) {
            loadClusterConfig();
        }
        int count = 0;
        String[] locations = new String[NCs.length * stores.length];
        for (String nc : NCs) {
            for (int i = 0; i < stores.length; i++) {
                locations[count] = nc;
                count++;
            }
        }
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, operator, locations);
    }

    /**
     * set location constraint
     * 
     * @param spec
     * @param operator
     * @throws HyracksDataException
     */
    public static void setCountConstraint(JobSpecification spec, IOperatorDescriptor operator) throws HyracksException {
        if (stores == null) {
            loadStores();
        }
        if (NCs == null) {
            loadClusterConfig();
        }
        int count = NCs.length * stores.length;
        PartitionConstraintHelper.addPartitionCountConstraint(spec, operator, count);
    }

    private static void loadClusterConfig() throws HyracksException {
        String line = "";
        ipToNcMapping = new HashMap<String, List<String>>();
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(confPath)));
            List<String> ncNames = new ArrayList<String>();
            while ((line = reader.readLine()) != null) {
                String[] ncConfig = line.split(" ");
                ncNames.add(ncConfig[1]);

                List<String> ncs = ipToNcMapping.get(ncConfig[0]);
                if (ncs == null) {
                    ncs = new ArrayList<String>();
                    ipToNcMapping.put(ncConfig[0], ncs);
                }
                ncs.add(ncConfig[1]);
            }
            NCs = ncNames.toArray(new String[0]);
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }
}