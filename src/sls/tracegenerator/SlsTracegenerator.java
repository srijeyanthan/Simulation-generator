package sls.tracegenerator;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;

/**
 *
 * @author sri
 */
/*
 Note : Generating the node configuration and job configurations for Distributed Load Simulation 
 */
public class SlsTracegenerator {

    private static final ArrayList<Integer> jobDistribtion = new ArrayList<>();
    private final String urlSufix = ".smile.com";
    private final int minimumContainerRunningTime = 2000; // this is in milli second
    private static final String EOL = System.getProperty("line.separator");
    private int numberOfNode = 0;
    private int jobLaunchTime = 0;
    private int singleJobTime = 0;
    private int numberOfContainers = 0;
    private int numberOfJobs = 0;
    private int jobIdSuffix = 1000;
    private static final Map<String, Set<String>> rackNodeMap
            = new TreeMap<>();
    private Map<Integer, Integer> containerStat = new HashMap<Integer, Integer>();
    private int totalContainers = 0;
    private int randomeNodeId = 0;
    private Map<Integer, SimulatedContainer> simulatedContainer = new HashMap<Integer, SimulatedContainer>();
    private Map<Integer, Integer> nodeManagerDistribution = new HashMap<Integer, Integer>();

    public SlsTracegenerator(int noOfNodes, int numberofapplication, int jobLaunchTime, int singleJobTime, int numberOfCon) {
        this.numberOfNode = noOfNodes;
        this.jobLaunchTime = jobLaunchTime;
        this.singleJobTime = singleJobTime;
        this.numberOfContainers = numberOfCon;
        this.numberOfJobs = numberofapplication;
        this.totalContainers = numberOfJobs * numberOfContainers;

    }

    public int getRandomNumber(int start, int end) {
        Random rand = new Random();
        int randomNumber = rand.nextInt((end - start) + 1) + start;
        return randomNumber;
    }

    public void updateContainerRunningMap(int startingTime, int endTime) {
        for (int i = startingTime; i <= endTime; ++i) {
            if (containerStat.containsKey(i)) {
                int previousCounter = containerStat.get(i);
                containerStat.put(i, ++previousCounter);
            } else {
                containerStat.put(i, 1);
            }
        }
    }

    public void createSLSTraceJobs(String machineId) throws IOException {
        int initalJobStartTime = 0;
        int jobEndTime = 100000;
        randomeNodeId = new Random().nextInt((numberOfNode - 1) + 1) + 1;
        int simulatedCounter = 0;
        Set<Integer> uniqueNodeManager = new HashSet<Integer>();
        Writer output = new FileWriter("sls-jobs.json");
        ObjectMapper mapper = new ObjectMapper();
        ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
        for (int n = 0; n < 6; ++n) {
            for (int m = 0; m < 6; ++m) {

                for (int i = 0; i < jobDistribtion.get(m) / 6; ++i) {
                    List array = new ArrayList();
                    Map json = new LinkedHashMap();
                    long l_lSeconds = System.currentTimeMillis() / 1000l;
                    String jobId = "job_" + l_lSeconds + "_" + jobIdSuffix;
                    ++jobIdSuffix;
                    int jobStartTime = getRandomNumber(initalJobStartTime, (jobEndTime - 20000));
                    json.put("am.type", "mapreduce");
                    json.put("job.start.ms", jobStartTime);
                    json.put("job.end.ms", jobEndTime);
                    json.put("job.queue.name", "sls_queue_1");
                    json.put("job.id", jobId);
                    json.put("job.user", "sri");

                    switch (m + 1) {
                        case 1:
                            numberOfContainers = getRandomNumber(1, 10);
                            break;
                        case 2:
                            numberOfContainers = getRandomNumber(10, 100);
                            break;
                        case 3:
                            numberOfContainers = getRandomNumber(100, 1000);
                            break;
                        case 4:
                            numberOfContainers = getRandomNumber(1000, 10000);
                            break;
                        case 5:
                            numberOfContainers = getRandomNumber(10000, 100000);
                            break;
                        case 6:
                            numberOfContainers = getRandomNumber(100000, 1000000);
                            break;
                        default:
                            break;
                    }

                    for (int j = 0; j < numberOfContainers; ++j) // number of containers
                    {
                        int localJobEndTime = 0;
                        int randomStartTime = getRandomNumber(jobStartTime, (jobEndTime - minimumContainerRunningTime));
                        if (randomStartTime + minimumContainerRunningTime >= jobEndTime) {
                            localJobEndTime = jobEndTime;
                        } else {
                            localJobEndTime = getRandomNumber(randomStartTime + minimumContainerRunningTime, jobEndTime);
                        }
                        Map task = new LinkedHashMap();
                        int randomHost = getRandomNumber(0, numberOfNode);
                        String hostName = "/default-rack/" + machineId + randomHost + urlSufix;
                        task.put("container.host", hostName);
                        task.put("container.start.ms", randomStartTime);
                        task.put("container.end.ms", localJobEndTime);
                        task.put("container.priority", 20);
                        task.put("container.type", "map");
                        array.add(task);
                        updateContainerRunningMap(randomStartTime / 1000, localJobEndTime / 1000);
                        if (randomeNodeId == randomHost) {
                            simulatedContainer.put(simulatedCounter, new SimulatedContainer(randomStartTime / 1000, localJobEndTime / 1000, hostName));
                        } else {
                            simulatedContainer.put(simulatedCounter, new SimulatedContainer(0, 0, "null"));
                        }
                        ++simulatedCounter;
                        uniqueNodeManager.add(randomHost);
                    }
                    nodeManagerDistribution.put(i, uniqueNodeManager.size());
                    uniqueNodeManager.clear();
                    json.put("job.tasks", array);

                    output.write(writer.writeValueAsString(json) + EOL);
                }

            }
            initalJobStartTime += 100000;
            jobEndTime = initalJobStartTime + 100000;
        }
        output.close();
    }

    public void createNodeManagerMap(int startingPos, String machineId) {
        Set<String> hosts = new TreeSet<String>();
        for (int i = startingPos; i < numberOfNode; ++i) {
            String hostName = machineId + i + urlSufix;
            hosts.add(hostName);
        }
        rackNodeMap.put("default-rack", hosts);
    }

    public void generateSLSNodeFile()
            throws IOException {

        Writer output = new FileWriter("sls-nodes.json");
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectWriter writer = mapper.defaultPrettyPrintingWriter();
            for (Map.Entry<String, Set<String>> entry : rackNodeMap.entrySet()) {
                Map rack = new LinkedHashMap();
                rack.put("rack", entry.getKey());
                List nodes = new ArrayList();
                for (String name : entry.getValue()) {
                    Map node = new LinkedHashMap();
                    node.put("node", name);
                    nodes.add(node);
                }
                rack.put("nodes", nodes);
                output.write(writer.writeValueAsString(rack) + EOL);
            }
        } finally {
            output.close();
        }
    }

    public void writeStatInToFile() throws IOException {
        Writer containerStatWriter = null;
        Writer nmStartWriter = null;
        try {
            containerStatWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("containerstat.txt"), "utf-8"));
            nmStartWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("nmdistributionstat.txt"), "utf-8"));
            int secCounter = 1;
            int totalNumberContainer = 0;
            for (Integer numberofContainer : containerStat.values()) {
                totalNumberContainer += numberofContainer;
            }
            containerStatWriter.write("time(sec),Running containers - Total Containers -" + totalNumberContainer + ", load percentage" + "\n");
            for (Integer counter : containerStat.values()) {
                float loadPercentage = (float) counter / (float) totalContainers;
                containerStatWriter.write(secCounter + "," + counter + "," + 100 * loadPercentage + "\n");
                ++secCounter;
            }
//            for (SimulatedContainer sc : simulatedContainer.values()) {
//                if (sc == null) {
//                    writer2.write("0,0" + "\n");
//                } else {
//                    writer2.write(sc.getStartTime() + "," + sc.getEndTime() + "\n");
//                }
//
//            }
            for (Integer nmdistribution : nodeManagerDistribution.values()) {
                nmStartWriter.write(nmdistribution + "\n");
            }
        } catch (IOException ex) {
            System.out.println(ex);
        } finally {
            containerStatWriter.close();
            nmStartWriter.close();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 7) {
            int numberOfNode = Integer.parseInt(args[0]);
            int numberofapplication = Integer.parseInt(args[1]);
            int jobLaunchTime = Integer.parseInt(args[2]);
            int singleJobTime = Integer.parseInt(args[3]);
            double containernodepercentage = Double.parseDouble(args[4]);

            int[] applicationDisPer = {60, 20, 12, 5, 2, 1};
            if (numberofapplication < 100) {
                System.out.println("Number of applications should be greater than 100");
                System.exit(-1);
            }
            for (int i = 0; i < applicationDisPer.length; ++i) {
                int qualifiedAppNumber = (int) ((applicationDisPer[i] * numberofapplication) / 100) % 6;
                if (qualifiedAppNumber != 0) {
                    System.out.println("Application distribution percentage is not good");
                    System.exit(-1);
                }
                // job distribtion has a number of total application in 10 min
                jobDistribtion.add((int) ((applicationDisPer[i] * numberofapplication) / 100));
                System.out.println("Number of jobs :: " + (int) ((applicationDisPer[i] * numberofapplication) / 100));
            }

            String machineId = args[5];
            int startingPos = Integer.parseInt(args[6]);
            int numberOfContainers = (int) (containernodepercentage * numberOfNode);
            System.out.println("Total containers : " + numberOfContainers * numberofapplication);
            // we are converting all the seconds to milliseconds
            SlsTracegenerator sls = new SlsTracegenerator(numberOfNode, numberofapplication, jobLaunchTime * 1000, singleJobTime * 1000, numberOfContainers);
            sls.createNodeManagerMap(startingPos, machineId);
            sls.generateSLSNodeFile();
            sls.createSLSTraceJobs(machineId);
            sls.writeStatInToFile();
        } else {
            System.out.println("Usage : java -jar sls-tracegenerator.jar <number of nodes> <total application> <job launch time> <single job time> <container node percentage> <machineid> <nmoffsetpos>");
            System.exit(0);
        }
    }

}
