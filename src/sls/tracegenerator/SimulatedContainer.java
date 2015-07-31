/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sls.tracegenerator;

/**
 *
 * @author sri
 */
public class SimulatedContainer {
    private final int startTime;
    private final int endTime;
    private final String runningHostId;

    public SimulatedContainer(int startTime, int endTime, String runningHostId) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.runningHostId = runningHostId;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public String getRunningHostId() {
        return runningHostId;
    }
    
}
