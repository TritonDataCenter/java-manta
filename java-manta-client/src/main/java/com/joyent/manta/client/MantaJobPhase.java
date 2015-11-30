/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Objects;

/**
 * Class representing either a map or a reduce phase of a Manta Job.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MantaJobPhase {
    private List<String> assets;
    private String exec;
    private String type;
    private String init;
    private Integer count;
    private Integer memory;
    private Integer disk;

    public MantaJobPhase() {

    }


    public MantaJobPhase(String exec, String type) {
        this.exec = exec;
        this.type = type;
    }


    public String getExec() {
        return exec;
    }


    public void setExec(String exec) {
        this.exec = exec;
    }


    public String getType() {
        return type;
    }


    public void setType(String type) {
        this.type = type;
    }


    public List<String> getAssets() {
        return assets;
    }

    public void setAssets(List<String> assets) {
        this.assets = assets;
    }

    public String getInit() {
        return init;
    }

    public void setInit(String init) {
        this.init = init;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getMemory() {
        return memory;
    }

    public void setMemory(Integer memory) {
        this.memory = memory;
    }

    public Integer getDisk() {
        return disk;
    }

    public void setDisk(Integer disk) {
        this.disk = disk;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantaJobPhase that = (MantaJobPhase) o;
        return Objects.equals(assets, that.assets) &&
                Objects.equals(exec, that.exec) &&
                Objects.equals(type, that.type) &&
                Objects.equals(init, that.init) &&
                Objects.equals(count, that.count) &&
                Objects.equals(memory, that.memory) &&
                Objects.equals(disk, that.disk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assets, exec, type, init, count, memory, disk);
    }

    @Override
    public String toString() {
        return "MantaJobPhase{" +
                "assets=" + assets +
                ", exec='" + exec + '\'' +
                ", type='" + type + '\'' +
                ", init='" + init + '\'' +
                ", count=" + count +
                ", memory=" + memory +
                ", disk=" + disk +
                '}';
    }
}
