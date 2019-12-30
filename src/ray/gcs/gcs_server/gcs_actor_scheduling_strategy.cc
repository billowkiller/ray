#include "gcs_actor_scheduling_strategy.h"
#include "gcs_actor_manager.h"
#include "gcs_node_manager.h"

namespace ray {

namespace gcs {

std::shared_ptr<GcsNode> GcsActorSchedulingStrategy::SelectNode(
    std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor);
  if (auto gcs_node_manager = gcs_node_manager_.lock()) {
    // TODO(zsl):
    return nullptr;
  }
  return nullptr;
}

}  // namespace gcs

}  // namespace ray