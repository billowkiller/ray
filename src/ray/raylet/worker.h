#ifndef RAY_RAYLET_WORKER_H
#define RAY_RAYLET_WORKER_H

#include <memory>

#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_common.h"
#include "ray/protobuf/common.pb.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace raylet {

/// Worker class encapsulates the implementation details of a worker. A worker
/// is the execution container around a unit of Ray work, such as a task or an
/// actor. Ray units of work execute in the context of a Worker.
class Worker {
 public:
  /// A constructor that initializes a worker object.
  Worker(const WorkerID &worker_id, pid_t pid, int port, const Language &language);
  /// A destructor responsible for freeing all worker state.
  ~Worker() {}
  void MarkAsKilling();
  bool IsKilling() const;
  void MarkBlocked();
  void MarkUnblocked();
  bool IsBlocked() const;
  /// Return the worker's PID.
  pid_t Pid() const;
  Language GetLanguage() const;
  const WorkerID &GetWorkerId() const;
  int Port() const;
  void AssignTaskId(const TaskID &task_id);
  const TaskID &GetAssignedTaskId() const;
  bool AddBlockedTaskId(const TaskID &task_id);
  bool RemoveBlockedTaskId(const TaskID &task_id);
  const std::unordered_set<TaskID> &GetBlockedTaskIds() const;
  void AssignJobId(const JobID &job_id);
  const JobID &GetAssignedJobId() const;
  void AssignActorId(const ActorID &actor_id);
  const ActorID &GetActorId() const;

  const ResourceIdSet &GetLifetimeResourceIds() const;
  void SetLifetimeResourceIds(ResourceIdSet &resource_ids);
  void ResetLifetimeResourceIds();

  const ResourceIdSet &GetTaskResourceIds() const;
  void SetTaskResourceIds(ResourceIdSet &resource_ids);
  void ResetTaskResourceIds();
  ResourceIdSet ReleaseTaskCpuResources();
  void AcquireTaskCpuResources(const ResourceIdSet &cpu_resources);

  int HeartbeatTimeout() { return ++heartbeat_timeout_times_; }
  void ClearHeartbeat() { heartbeat_timeout_times_ = 0; }

 private:
  /// The worker's ID.
  WorkerID worker_id_;
  /// The worker's PID.
  pid_t pid_;
  /// The worker port.
  int port_;
  /// The language type of this worker.
  Language language_;
  /// The worker's currently assigned task.
  TaskID assigned_task_id_;
  /// Job ID for the worker's current assigned task.
  JobID assigned_job_id_;
  /// The worker's actor ID. If this is nil, then the worker is not an actor.
  ActorID actor_id_;
  /// Whether the worker is blocked. Workers become blocked in a `ray.get`, if
  /// they require a data dependency while executing a task.
  bool blocked_;
  /// The specific resource IDs that this worker owns for its lifetime. This is
  /// only used for actors.
  ResourceIdSet lifetime_resource_ids_;
  /// The specific resource IDs that this worker currently owns for the duration
  // of a task.
  ResourceIdSet task_resource_ids_;
  std::unordered_set<TaskID> blocked_task_ids_;
  /// Record worker heartbeat timeout times.
  int heartbeat_timeout_times_;
  /// Indicate we have sent kill signal to the worker if it's true. We cannot treat the
  /// worker process as reaylly dead until we lost the heartbeats from the worker.
  bool is_killing_;
};

}  // namespace raylet

}  // namespace ray

#endif  // RAY_RAYLET_WORKER_H
