/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.Validate;

import java.util.List;
import java.util.Objects;

/**
 * Class representing either a map or a reduce phase of a Manta Job.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MantaJobPhase {
    /**
     * List of Manta objects to include in job as assets.
     */
    private List<String> assets;

    /**
     * Command to execute as job phase.
     */
    private String exec;

    /**
     * Phase type - either "map" or "reduce".
     */
    private String type;

    /**
     * Initial command to execute before exec command is executed.
     */
    private String init;

    /**
     * An optional number of reducers for this phase (reduce-only).
     */
    private Integer count;

    /**
     * An optional amount of DRAM to give to your compute zone (MB).
     */
    private Integer memory;

    /**
     * An optional amount of disk space to give to your compute zone (GB).
     */
    private Integer disk;


    /**
     * Constructor used when creating a phase for starting a new job.
     * Use the fluent setters to set properties.
     */
    public MantaJobPhase() {
    }

    /**
     * @return command to execute as job phase
     */
    public String getExec() {
        return exec;
    }

    /**
     * @param exec command to execute as job phase
     * @return reference to the current instance
     */
    public MantaJobPhase setExec(final String exec) {
        this.exec = exec;
        return this;
    }

    /**
     * @return Phase type - either "map" or "reduce"
     */
    public String getType() {
        return type;
    }

    /**
     * @param type phase type - either "map" or "reduce"
     * @return reference to the current instance
     */
    public MantaJobPhase setType(final String type) {
        Validate.notNull(type, "Type must not be null");

        if (!(type.equals("map") || type.equals("reduce"))) {
            throw new IllegalArgumentException("Type must equal map or reduce");
        }

        this.type = type;
        return this;
    }

    /**
     * @return list of Manta objects to include in job as assets
     */
    public List<String> getAssets() {
        return assets;
    }

    /**
     * @param assets list of Manta objects to include in job as assets
     * @return reference to the current instance
     */
    public MantaJobPhase setAssets(final List<String> assets) {
        this.assets = assets;
        return this;
    }

    /**
     * @return initial command to execute before exec command is executed
     */
    public String getInit() {
        return init;
    }

    /**
     * @param init initial command to execute before exec command is executed
     * @return reference to the current instance
     */
    public MantaJobPhase setInit(final String init) {
        this.init = init;
        return this;
    }

    /**
     * @return an optional number of reducers for this phase (reduce-only)
     */
    public Integer getCount() {
        return count;
    }

    /**
     * @param count an optional number of reducers for this phase (reduce-only)
     * @return reference to the current instance
     */
    public MantaJobPhase setCount(final Integer count) {
        if (getType() != null && !getType().equals("reduce")) {
            throw new IllegalArgumentException("Count can only be set for the reduce phase");
        }

        if (count != null && count < 1) {
            throw new IllegalArgumentException("Count must be null or greater than 1");
        }

        this.count = count;
        return this;
    }

    /**
     * @return an optional amount of DRAM to give to your compute zone (MB)
     */
    public Integer getMemory() {
        return memory;
    }

    /**
     * @param memory an optional amount of DRAM to give to your compute zone (MB)
     * @return reference to the current instance
     */
    public MantaJobPhase setMemory(final Integer memory) {
        if (memory != null && memory < 1) {
            throw new IllegalArgumentException("Count must be null or greater than 1");
        }

        this.memory = memory;
        return this;
    }

    /**
     * @return an optional amount of disk space to give to your compute zone (GB)
     */
    public Integer getDisk() {
        return disk;
    }

    /**
     * @param disk an optional amount of disk space to give to your compute zone (GB)
     * @return reference to the current instance
     */
    public MantaJobPhase setDisk(final Integer disk) {
        if (disk != null && disk < 1) {
            throw new IllegalArgumentException("Count must be null or greater than 1");
        }

        this.disk = disk;
        return this;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        MantaJobPhase that = (MantaJobPhase) other;
        return Objects.equals(assets, that.assets)
                && Objects.equals(exec, that.exec)
                && Objects.equals(type, that.type)
                && Objects.equals(init, that.init)
                && Objects.equals(count, that.count)
                && Objects.equals(memory, that.memory)
                && Objects.equals(disk, that.disk);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assets, exec, type, init, count, memory, disk);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MantaJobPhase{");
        sb.append("assets=").append(assets);
        sb.append(", exec='").append(exec).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", init='").append(init).append('\'');
        sb.append(", count=").append(count);
        sb.append(", memory=").append(memory);
        sb.append(", disk=").append(disk);
        sb.append('}');
        return sb.toString();
    }
}
