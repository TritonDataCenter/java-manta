/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.util.Key;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Class representing a Manta job.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJob {
//    @Key("id")
    private UUID id;
    @Key
    private String name;
    @Key
    private String state;
    @Key
    private Boolean cancelled;
    @Key
    private Boolean inputDone;
    @Key("transient")
    private Boolean tranzient;
    @Key
    private Map<String, Number> stats;
    @Key("timeCreated")
    private Instant timeCreated;
    private Instant timeDone;
    private Instant timeArchiveStarted;
    private Instant timeArchiveDone;
    @Key
    private List<MantaJobPhase> phases;
    @Key
    private Map<String, Object> options;

    public MantaJob() {
    }


    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Boolean getCancelled() {
        return cancelled;
    }

    public void setCancelled(Boolean cancelled) {
        this.cancelled = cancelled;
    }

    public Boolean getInputDone() {
        return inputDone;
    }

    public void setInputDone(Boolean inputDone) {
        this.inputDone = inputDone;
    }

    public Instant getTimeCreated() {
        return timeCreated;
    }

    public void setTimeCreated(Instant timeCreated) {
        this.timeCreated = timeCreated;
    }

    public Instant getTimeDone() {
        return timeDone;
    }

    public void setTimeDone(Instant timeDone) {
        this.timeDone = timeDone;
    }

    public List<MantaJobPhase> getPhases() {
        return phases;
    }

    public void setPhases(List<MantaJobPhase> phases) {
        this.phases = phases;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantaJob mantaJob = (MantaJob) o;
        return Objects.equals(id, mantaJob.id) &&
                Objects.equals(name, mantaJob.name) &&
                Objects.equals(state, mantaJob.state) &&
                Objects.equals(cancelled, mantaJob.cancelled) &&
                Objects.equals(inputDone, mantaJob.inputDone) &&
                Objects.equals(timeCreated, mantaJob.timeCreated) &&
                Objects.equals(timeDone, mantaJob.timeDone) &&
                Objects.equals(phases, mantaJob.phases);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, state, cancelled, inputDone,
                timeCreated, timeDone, phases);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MantaJob{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", state='").append(state).append('\'');
        sb.append(", cancelled=").append(cancelled);
        sb.append(", inputDone=").append(inputDone);
        sb.append(", timeCreated=").append(timeCreated);
        sb.append(", timeDone=").append(timeDone);
        sb.append(", phases=").append(phases);
        sb.append('}');
        return sb.toString();
    }
}
