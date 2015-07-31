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
    private Map<Integer, Integer> containerDistribution = new HashMap<Integer, Integer>();
    private final int[][] probabilityMatrix = new int[5][6];
    private final int[][] jobsDistributionMatix = new int[5][6];
    private Map<Integer,Integer> appDistribution = new HashMap<Integer, Integer>();
    public SlsTracegenerator(int noOfNodes, int numberofapplication, int jobLaunchTime, int singleJobTime, int numberOfCon) {
        this.numberOfNode = noOfNodes;
        this.jobLaunchTime = jobLaunchTime;
        this.singleJobTime = singleJobTime;
        this.numberOfContainers = numberOfCon;
        this.numberOfJobs = numberofapplication;
        this.totalContainers = numberOfJobs * numberOfContainers;
        for(int i=0;i<600;++i)
            appDistribution.put(i, 0);

    }

    public void fillProbabilityMatrix(int col, int row) {

        int[] zeroColms = new int[6];
        for (int i = 0; i < row; ++i) {
            for (int j = 0; j < col; ++j) {
                //only for last one
                if (i == row - 1) {
                    int random = getRandomNumber(0, 1);
                    probabilityMatrix[i][j] = random;
                    zeroColms[j] = random;
                    System.out.println("row - " + i + " col - " + j + " val = " + random);
                } else {
                    System.out.println("row - " + i + " col - " + j);
                    probabilityMatrix[i][j] = 1;
                }
            }
        }

        for (int i = 0; i < col; ++i) {
            for (int j = 0; j < row; ++j) {
                jobsDistributionMatix[j][i] = jobDistribtion.get(j) / 6;
            }
            if(zeroColms[i] == 0)
                jobsDistributionMatix[2][i]=26;
        }
    }

    private int getElement(int i, int j) {
        return probabilityMatrix[i][j];
    }

    private int getRandomNumber(int start, int end) {
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
    private void updateAppsDistribution(int jobStartTime){
         int prefix = jobStartTime/1000;
         if(appDistribution.containsKey(prefix)){
             appDistribution.put(prefix, appDistribution.get(prefix)+1);
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
            System.out.println("Generating the load to segment - " + (n + 1));
            for (int m = 0; m < 5; ++m) {
                if (getElement(m, n) == 0) {
                    continue;
                }
                int numberOfJobs = jobsDistributionMatix[m][n];
                System.out.println("Number of jobs " + numberOfJobs);
                for (int i = 0; i < numberOfJobs; ++i) {
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
                     
                    updateAppsDistribution(jobStartTime);
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
                    if (containerDistribution.containsKey(m + 1)) {
                        containerDistribution.put(m + 1, containerDistribution.get(m + 1) + numberOfContainers);
                    } else {
                        containerDistribution.put(m + 1, numberOfContainers);
                    }

                    //System.out.println(" ===== Number of container :"+numberOfContainers + "|number of jobs : "+numberOfJobs);
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
        int totalContainers = 0;
        for (Integer values : containerDistribution.values()) {
            totalContainers += values;
        }
        int range = 1;
        for (Integer values : containerDistribution.values()) {
            System.out.println("Range - " + range + " value -" + values + " percentage : " + (float) (values*100) / totalContainers);
            ++range;
        }
        int counter=0;
        int totalapp=0;
        for(Integer appsValue :appDistribution.values()){
            totalapp +=appsValue;
            ++counter;
           // if(counter%10 ==0){
            System.out.println(totalapp);
            totalapp=0;
            //}
            
        }
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

            int[] applicationDisPer = {54, 18, 25, 2, 1};
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
                System.out.println("Number of jobsjobsDistributionMatix :: " + (int) ((applicationDisPer[i] * numberofapplication) / 100));
            }

            String machineId = args[5];
            int startingPos = Integer.parseInt(args[6]);
            int numberOfContainers = (int) (containernodepercentage * numberOfNode);
            System.out.println("Total containers : " + numberOfContainers * numberofapplication);
            // we are converting all the seconds to milliseconds
            SlsTracegenerator sls = new SlsTracegenerator(numberOfNode, numberofapplication, jobLaunchTime * 1000, singleJobTime * 1000, numberOfContainers);
            sls.fillProbabilityMatrix(6, 5);
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

