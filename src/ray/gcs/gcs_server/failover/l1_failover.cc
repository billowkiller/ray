#include "l1_failover.h"
#include "ray/protobuf/failover.pb.h"
#include "ray/rpc/failover/failover_client.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace gcs {

L1Failover::L1Failover(boost::asio::io_context &ioc, fd::FailureDetectorMaster &fd)
    : ioc_(ioc), fd_(fd) {
  fd_.OnWorkerConnected([this](ray::fd::WorkerContext &&ctx) {
    OnWorkerConnected(ctx.node_id, ctx.endpoint);
  });

  fd_.OnWorkerDisconnected([this](std::vector<ray::fd::WorkerContext> &&ctxs) {
    for (auto &ctx : ctxs) {
      OnWorkerDisconnected(ctx.node_id, ctx.endpoint);
    }
  });

  fd_.OnWorkerRestartedWithinLease([this](ray::fd::WorkerContext &&ctx) {
    OnWorkerRestarted(ctx.node_id, ctx.endpoint);
  });
}

bool L1Failover::TryRegister(const ray::rpc::RegisterRequest &req,
                             ray::rpc::RegisterReply *reply) {
  auto ep = endpoint_from_uint64(req.address());
  bool success = false;
  do {
    // Do not issue new token if fo in progress
    if (!round_failed_nodes_.empty()) {
      break;
    }

    // Issue a new token for the node if the node has no token
    auto iter = registered_nodes_.find(req.node_id());
    if (iter == registered_nodes_.end()) {
      NodeContext ctx;
      ctx.node_id = req.node_id();
      ctx.ep = ep;
      ctx.secret = req.secret();
      AcceptNode(ctx);
      success = true;
      break;
    }

    success = (iter->second.secret == req.secret());
  } while (0);

  RAY_LOG(INFO) << "[" << __FUNCTION__ << "] nid: " << req.node_id()
                << ", address: " << ep.to_string() << ", secret: " << req.secret()
                << ", failed_nodes_cnt: " << round_failed_nodes_.size()
                << ", try register " << (success ? "success!" : "failed!");

  reply->set_success(success);
  return success;
}

void L1Failover::OnNodeFailed(const AbstractFailover::NodeContext &ctx) {
  if (!round_failed_nodes_.emplace(ctx.node_id).second) {
    return;
  }

  if (round_failed_nodes_.size() == 1) {
    OnRoundFailedBegin(ctx);
  }

  if (round_failed_nodes_.size() == registered_nodes_.size()) {
    OnRoundFailedEnd(ctx);
  }
}

void L1Failover::AcceptNode(const AbstractFailover::NodeContext &ctx) {
  RAY_LOG(INFO) << "[" << __FUNCTION__ << "] node_id: " << ctx.node_id
                << ", address: " << ctx.ep.to_string();
  registered_nodes_[ctx.node_id] = ctx;
}

void L1Failover::OnRoundFailedBegin(const AbstractFailover::NodeContext &ctx) {
  RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] **** Abnormal exit detected ****"
                   << " node_id: " << ctx.node_id << ", address: " << ctx.ep.to_string()
                   << ", registered_node_cnt: " << registered_nodes_.size();
  // fd_.PauseOnMaster();

  if (on_round_failed_begin_) {
    on_round_failed_begin_();
  }

  for (auto &entry : registered_nodes_) {
    if (entry.first != ctx.node_id) {
      RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] reset node: " << entry.second.node_id
                       << ", address: " << entry.second.ep.to_string() << " as node("
                       << ctx.node_id << ", " << ctx.ep.to_string() << ") failed!";
      ResetNode(entry.second);
    }
  }
}

void L1Failover::OnRoundFailedEnd(const AbstractFailover::NodeContext &ctx) {
  RAY_LOG(INFO) << "[" << __FUNCTION__ << "] node_id: " << ctx.node_id;
  registered_nodes_.clear();
  round_failed_nodes_.clear();

  if (on_round_failed_end_) {
    on_round_failed_end_();
  }

  // fd_.ResumeOnMaster();
}

void L1Failover::ResetNode(const NodeContext &ctx) {
  rpc::ResetStateRequest req;
  req.set_secret(ctx.secret);

  auto client = std::make_shared<rpc::FailoverAsioClient>(ctx.ep.address().to_string(),
                                                          ctx.ep.port(), ioc_);
  auto status = client->ResetState(
      req, [](const Status &status, const rpc::ResetStateReply &reply) {
        // just ignore this callback in L1Failover
      });
  if (!status.ok()) {
    RAY_LOG(INFO) << "[" << __FUNCTION__ << "] failed, just ignore!"
                  << " node_id: " << ctx.node_id
                  << ", address: " << ctx.ep.address().to_string()
                  << ", error: " << status;
    return;
  }

  auto timer = std::make_shared<boost::asio::deadline_timer>(ioc_);
  timer->expires_from_now(boost::posix_time::milliseconds(3000));
  timer->async_wait(
      [client, timer](const boost::system::error_code &error) { RAY_CHECK(!error); });
}

}  // namespace gcs
}  // namespace ray
