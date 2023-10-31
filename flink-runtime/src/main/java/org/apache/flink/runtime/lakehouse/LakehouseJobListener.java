/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.lakehouse;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.JobStatusListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.apache.flink.api.common.JobStatus.CANCELED;
import static org.apache.flink.api.common.JobStatus.FAILED;
import static org.apache.flink.api.common.JobStatus.FAILING;
import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.apache.flink.api.common.JobStatus.SUSPENDED;

/** LakehouseJobListener is a listener for job status changes. */
public class LakehouseJobListener implements JobStatusListener {

    static final Logger LOG = LoggerFactory.getLogger(LakehouseJobListener.class);

    public static Consumer<String> c = null;

    @Override
    public void jobStatusChanges(
            JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
        // send job status to lakehouse
        LOG.warn(
                "job: ["
                        + jobId.toString()
                        + "] status changes, lakehouse job listener: "
                        + newJobStatus);
        if (isFinalState(newJobStatus)) {
            LOG.warn("-----------------------------------------------------------");
            LOG.warn(
                    "job: ["
                            + jobId.toString()
                            + "] is final state, lakehouse job listener: "
                            + newJobStatus);
            callBackWhenFinal(jobId.toString());
        }
    }

    private boolean isFinalState(JobStatus status) {
        return Arrays.asList(FAILING, FAILED, CANCELED, FINISHED, SUSPENDED).contains(status);
    }

    private void callBackWhenFinal(String jobId) {
        if (c != null) {
            c.accept(jobId);
        } else {
            LOG.warn("lakehouse job callback is null ------------------------------");
        }
    }
}
