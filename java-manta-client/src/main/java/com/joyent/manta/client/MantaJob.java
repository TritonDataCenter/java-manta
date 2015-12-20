/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class representing a Manta job.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MantaJob {
    /**
     * Manta job ID.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private UUID id;

    /**
     * Name of Manta job.
     */
    private String name;

    /**
     * Manta job state (running, done, etc).
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String state;

    /**
     * Flag indicating that the job was cancelled.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Boolean cancelled;

    /**
     * Flag indicating whether or not new input is accepted.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Boolean inputDone;

    /**
     * Flag indicating that a job is transient.
     */
    @JsonProperty(value = "transient", access = JsonProperty.Access.WRITE_ONLY)
    private Boolean tranzient;

    /**
     * Statistics about job.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Map<String, Number> stats;

    /**
     * Creation time of job.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Instant timeCreated;

    /**
     * Timestamp of when the job finished.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Instant timeDone;

    /**
     * Timestamp of when the job was archived.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Instant timeArchiveStarted;

    /**
     * Flag indicating that the job was archived.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Instant timeArchiveDone;

    /**
     * List of job phases associated with job (map/reduce).
     */
    private List<MantaJobPhase> phases;

    /**
     * List of options associated with job.
     */
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private Map<String, Object> options;


    /**
     * Package scope constructor used when deserializing from JSON.
     */
    MantaJob() {
    }


    /**
     * Constructor used when creating a new Manta job to be executed.
     * @param name name of job
     * @param phases map and/or reduce phases
     */
    public MantaJob(final String name, final List<MantaJobPhase> phases) {
        this.name = name;
        this.phases = phases;
    }

    /**
     * @return Manta job ID
     */
    public UUID getId() {
        return id;
    }


    /**
     * @return Name of Manta job
     */
    public String getName() {
        return name;
    }


    /**
     * @return Manta job state (running, done, etc)
     */
    public String getState() {
        return state;
    }


    /**
     * @return flag indicating that a job is transient
     */
    public Boolean getTransient() {
        return tranzient;
    }


    /**
     * @return statistics about job
     */
    public Map<String, Number> getStats() {
        return stats;
    }


    /**
     * @return timestamp of when the job was archived
     */
    public Instant getTimeArchiveStarted() {
        return timeArchiveStarted;
    }


    /**
     * @return timestamp of when the job finished
     */
    public Instant getTimeArchiveDone() {
        return timeArchiveDone;
    }


    /**
     * @return list of options associated with job
     */
    public Map<String, Object> getOptions() {
        return options;
    }


    /**
     * @return flag indicating that the job was cancelled
     */
    public Boolean getCancelled() {
        return cancelled;
    }


    /**
     * @return flag indicating whether or not new input is accepted
     */
    public Boolean getInputDone() {
        return inputDone;
    }


    /**
     * @return creation time of job
     */
    public Instant getTimeCreated() {
        return timeCreated;
    }


    /**
     * @return timestamp of when the job finished
     */
    public Instant getTimeDone() {
        return timeDone;
    }


    /**
     * @return list of job phases associated with job (map/reduce)
     */
    public List<MantaJobPhase> getPhases() {
        return phases;
    }


    /**
     * List of all of the map phases of the job.
     *
     * @return list of map phases
     */
    @JsonIgnore
    public List<MantaJobPhase> getMapPhases() {
        Stream<MantaJobPhase> stream = phases.stream()
                .filter(p -> p.getType().equals("map"));

        return stream.collect(Collectors.toCollection(ArrayList::new));
    }


    /**
     * List of all of the reduce phases of the job.
     *
     * @return list of reduce phases
     */
    @JsonIgnore
    public List<MantaJobPhase> getReducePhases() {
        Stream<MantaJobPhase> stream = phases.stream()
                .filter(p -> p.getType().equals("reduce"));

        return stream.collect(Collectors.toCollection(ArrayList::new));
    }


    @Override
    public boolean equals(final Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        MantaJob mantaJob = (MantaJob) that;
        return Objects.equals(id, mantaJob.id)
                && Objects.equals(name, mantaJob.name)
                && Objects.equals(state, mantaJob.state)
                && Objects.equals(cancelled, mantaJob.cancelled)
                && Objects.equals(inputDone, mantaJob.inputDone)
                && Objects.equals(timeCreated, mantaJob.timeCreated)
                && Objects.equals(timeDone, mantaJob.timeDone)
                && Objects.equals(phases, mantaJob.phases);
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
