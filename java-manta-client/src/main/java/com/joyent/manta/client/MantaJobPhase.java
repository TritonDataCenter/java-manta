/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.util.Key;

import java.util.List;
import java.util.Objects;

/**
 * Class representing either a map or a reduce phase of a Manta Job.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobPhase {
    @Key
    private List<String> assets;
    @Key
    private String exec;
    @Key
    private String type;


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantaJobPhase that = (MantaJobPhase) o;
        return Objects.equals(assets, that.assets) &&
                Objects.equals(exec, that.exec) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assets, exec, type);
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MantaJobPhase{");
        sb.append("assets=").append(assets);
        sb.append(", exec='").append(exec).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
