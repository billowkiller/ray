#include "ray/gcs/job_state_accessor.h"
#include <memory>
#include "gtest/gtest.h"
#include "ray/gcs/accessor_test_base.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/test_util.h"

namespace ray {

namespace gcs {

class JobStateAccessorTest : public AccessorTestBase<JobID, JobTableData> {
 protected:
  virtual void GenTestData() {
    for (size_t i = 0; i < total_job_number_; ++i) {
      JobID job_id = JobID::FromInt(i);
      std::shared_ptr<JobTableData> job_data_ptr = std::make_shared<JobTableData>();
      job_data_ptr->set_job_id(job_id.Binary());
      job_data_ptr->set_is_dead(false);
      job_data_ptr->set_timestamp(1);
      job_data_ptr->set_driver_pid(i);
      id_to_data_[job_id] = job_data_ptr;
    }
  }
  std::atomic<int> subscribe_pending_count_{0};
  size_t total_job_number_{100};
};

TEST_F(JobStateAccessorTest, AddAndSubscribe) {
  JobStateAccessor &job_accessor = gcs_client_->Jobs();
  // SubscribeAll
  auto on_subscribe = [this](const JobID &job_id, const JobTableData &data) {
    const auto it = id_to_data_.find(job_id);
    RAY_CHECK(it != id_to_data_.end());
    ASSERT_TRUE(data.is_dead());
    --subscribe_pending_count_;
  };

  auto on_done = [this](Status status) {
    RAY_CHECK_OK(status);
    --pending_count_;
  };

  ++pending_count_;
  RAY_CHECK_OK(job_accessor.AsyncSubscribeToFinishedJobs(on_subscribe, on_done));

  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);

  // Register
  for (const auto &item : id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(job_accessor.AsyncAdd(item.second, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);

  // Update
  for (auto &item : id_to_data_) {
    ++pending_count_;
    ++subscribe_pending_count_;
    RAY_CHECK_OK(job_accessor.AsyncMarkFinished(item.first, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }
  WaitPendingDone(wait_pending_timeout_);
  WaitPendingDone(subscribe_pending_count_, wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray
