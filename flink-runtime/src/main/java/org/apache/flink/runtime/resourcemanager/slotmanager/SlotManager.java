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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or an allocated
 * slot is freed, then it tries to fulfill another pending slot request. Whenever there are not
 * enough slots available the slot manager will notify the resource manager about it via {@link
 * ResourceActions#allocateResource(WorkerResourceSpec)}.
 *
 * <p>In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and pending slot requests time out triggering their release and
 * failure, respectively.
 *
 * 多易教育：
 * ResourceManager 借助 SlotManager 来管理 slot。 SlotManager 维护了所有已经注册的 TaskExecutor 的所有 slot 的状态，它们的分配情况。
 * SlotManager 还维护了所有处于等待状态的 slot 请求。
 * 每当有一个新的 slot 注册或者一个已经分配的 slot 被释放的时候，SlotManager 会试图去满足处于等待状态 slot request。
 * 如果可用的 slot 不足以满足要求，SlotManager 会通过 ResourceActions#allocateResource(ResourceProfile) 来告知 ResourceManager,
 * ResourceManager 可能会尝试启动新的 TaskExecutor (如 Yarn 模式下)。
 * 此外，长时间处于空闲状态的 TaskExecutor 或者长时间没有被满足的 pending slot request，会触发超时机制进行处理。
 */
public interface SlotManager extends AutoCloseable {
    int getNumberRegisteredSlots();

    int getNumberRegisteredSlotsOf(InstanceID instanceId);

    int getNumberFreeSlots();

    int getNumberFreeSlotsOf(InstanceID instanceId);

    /**
     * Get number of workers SlotManager requested from {@link ResourceActions} that are not yet
     * fulfilled.
     *
     * @return a map whose key set is all the unique resource specs of the pending workers, and the
     *     corresponding value is number of pending workers of that resource spec.
     */
    Map<WorkerResourceSpec, Integer> getRequiredResources();

    ResourceProfile getRegisteredResource();

    ResourceProfile getRegisteredResourceOf(InstanceID instanceID);

    ResourceProfile getFreeResource();

    ResourceProfile getFreeResourceOf(InstanceID instanceID);

    Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID);

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceActions to use for resource (de-)allocations
     */
    void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceActions newResourceActions);

    /** Suspends the component. This clears the internal state of the slot manager. */
    void suspend();

    /**
     * Notifies the slot manager that the resource requirements for the given job should be cleared.
     * The slot manager may assume that no further updates to the resource requirements will occur.
     *
     * @param jobId job for which to clear the requirements
     */
    void clearResourceRequirements(JobID jobId);

    /**
     * Notifies the slot manager about the resource requirements of a job.
     *
     * @param resourceRequirements resource requirements of a job
     */
    void processResourceRequirements(ResourceRequirements resourceRequirements);

    /**
     * Registers a new task manager at the slot manager. This will make the task managers slots
     * known and, thus, available for allocation.
     *
     * @param taskExecutorConnection for the new task manager
     * @param initialSlotReport for the new task manager
     * @param totalResourceProfile for the new task manager
     * @param defaultSlotResourceProfile for the new task manager
     * @return True if the task manager has not been registered before and is registered
     *     successfully; otherwise false
     */
    boolean registerTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile);

    /**
     * Unregisters the task manager identified by the given instance id and its associated slots
     * from the slot manager.
     *
     * @param instanceId identifying the task manager to unregister
     * @param cause for unregistering the TaskManager
     * @return True if there existed a registered task manager with the given instance id
     */
    boolean unregisterTaskManager(InstanceID instanceId, Exception cause);

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be marked as free and will be subject to new slot requests.
     *
     * @param slotId identifying the slot to free
     * @param allocationId with which the slot is presumably allocated
     */
    void freeSlot(SlotID slotId, AllocationID allocationId);

    void setFailUnfulfillableRequest(boolean failUnfulfillableRequest);
}
