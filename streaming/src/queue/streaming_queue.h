#ifndef _STREAMING_QUEUE_H_
#define _STREAMING_QUEUE_H_
#include <iterator>
#include <list>
#include <vector>

#include "ray/common/id.h"
#include "ray/util/util.h"

#include "streaming_logging.h"
#include "streaming_queue_item.h"
#include "transport.h"
#include "utils.h"

namespace ray {
namespace streaming {

using ray::ObjectID;

enum QueueType { UPSTREAM = 0, DOWNSTREAM };

/// A queue-like data structure, which do not delete it's item
/// after poped. The lifecycle of each item is:
/// - Pending, a item is pushed into queue, but has not been
///   processed (sent out or consumed),
/// - Processed, has been handled by user, but should not be deleted
/// - Evicted, useless to user, should be poped and destroyed.
/// At present, this data structure is implemented with one std::list,
/// using a watershed iterator to divided.
class Queue {
 public:
  /// \param size max size of the queue in bytes.
  Queue(uint64_t size, std::shared_ptr<Transport> transport)
      : max_data_size_(size), data_size_(0), data_size_sent_(0) {
    buffer_queue_.push_back(NullQueueItem());
    watershed_iter_ = buffer_queue_.begin();
  }

  virtual ~Queue() {}

  /// Push item into queue, return false is queue is full.
  bool Push(QueueItem item);

  /// Get the front of item which in processed state.
  QueueItem FrontProcessed();

  /// Pop the front of item which in processed state.
  QueueItem PopProcessed();

  /// Pop the front of item which in pending state, the item
  /// will not be evicted at this moment, its state turn to
  /// processed.
  QueueItem PopPending();

  /// PopPending with timeout in microseconds.
  QueueItem PopPendingBlockTimeout(uint64_t timeout_us);

  /// Return the last item in pending state.
  QueueItem BackPending();

  bool IsPendingEmpty();
  bool IsPendingFull(uint64_t data_size = 0);

  /// Return the size in bytes of all items in queue.
  uint64_t QueueSize() { return data_size_; }

  /// Return the size in bytes of all items in pending state.
  uint64_t PendingDataSize() { return data_size_ - data_size_sent_; }

  /// Return the size in bytes of all items in processed state.
  uint64_t ProcessedDataSize() { return data_size_sent_; }

  /// Return item count of the queue.
  size_t Count() { return buffer_queue_.size(); }

  /// Return item count in pending state.
  size_t PendingCount();

  /// Return item count in processed state.
  size_t ProcessedCount();

 protected:
  // TODO: lock-free list
  // Using list to implement: Push/Pop/Traverse
  std::list<QueueItem> buffer_queue_;

  std::list<QueueItem>::iterator watershed_iter_;

  // max data size in bytes
  uint64_t max_data_size_;
  uint64_t data_size_;
  uint64_t data_size_sent_;

  std::mutex mutex_;
  std::condition_variable readable_cv_;
};

/// Queue in upstream.
class WriterQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param size, max data size in bytes
  /// \param transport, transport
  WriterQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, uint64_t size, bool clear,
              std::shared_ptr<Transport> transport)
      : Queue(size, transport),
        actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        eviction_limit_(QUEUE_INVALID_SEQ_ID),
        min_consumed_id_(QUEUE_INVALID_SEQ_ID),
        peer_last_msg_id_(0),
        peer_last_seq_id_(QUEUE_INVALID_SEQ_ID),
        clear_(clear),
        transport_(transport),
        is_pulling_(false) {}

  /// Push a continuous buffer into queue.
  /// NOTE: the buffer should be copied.
  Status Push(uint64_t seq_id, uint8_t *data, uint32_t data_size, uint64_t timestamp,
              bool raw = false);

  /// Callback function, will be called when downstream queue notifies
  /// it has consumed some items.
  /// NOTE: this callback function is called in queue thread.
  void OnNotify(std::shared_ptr<NotificationMessage> notify_msg);

  /// Callback function, will be called when downstream queue wants to
  /// pull some items form our upstream queue.
  /// NOTE: this callback function is called in queue thread.
  std::shared_ptr<LocalMemoryBuffer> OnPull(std::shared_ptr<PullRequestMessage> pull_msg, bool async, boost::asio::io_service &service);

  /// Send items through direct call.
  void Send();

  /// Just for tests.
  uint64_t MockSend();

  /// Called when user pushs item into queue. The count of items
  /// can be evicted, determined by eviction_limit_ and min_consumed_id_.
  Status TryEvictItems();

  void SetQueueEvictionLimit(uint64_t eviction_limit) {
    eviction_limit_ = eviction_limit;
  }

  uint64_t EvictionLimit() { return eviction_limit_; }

  void SetMinConsumedSeqId(uint64_t seq_id) { min_consumed_id_ = seq_id; }

  uint64_t GetMinConsumedSeqID() { return min_consumed_id_; }

  void SetPeerLastIds(uint64_t msg_id, uint64_t seq_id) { 
    peer_last_msg_id_ = msg_id; 
    peer_last_seq_id_ = seq_id;
  }

  uint64_t GetPeerLastMsgId() { return peer_last_msg_id_; }

  uint64_t GetPeerLastSeqId() { return peer_last_seq_id_; }

  bool IsClear() { return clear_; }

 private:
  void SendPullItem(QueueItem &item, uint64_t first_seq_id, uint64_t last_seq_id);
  int SendPullItems(uint64_t target_seq_id, uint64_t first_seq_id, uint64_t last_seq_id);
  void CreateSendMsgTask(QueueItem &item, std::vector<TaskArg> &args);

 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  uint64_t eviction_limit_;
  uint64_t min_consumed_id_;
  uint64_t peer_last_msg_id_;
  uint64_t peer_last_seq_id_;
  bool clear_;
  std::shared_ptr<Transport> transport_;

  std::atomic<bool> is_pulling_;
};

/// Queue in downstream.
class ReaderQueue : public Queue {
 public:
  /// \param queue_id, the unique ObjectID to identify a queue
  /// \param actor_id, the actor id of upstream worker
  /// \param peer_actor_id, the actor id of downstream worker
  /// \param transport, transport
  /// NOTE: we do not restrict queue size of ReaderQueue
  ReaderQueue(const ObjectID &queue_id, const ActorID &actor_id,
              const ActorID &peer_actor_id, std::shared_ptr<Transport> transport)
      : Queue(std::numeric_limits<uint64_t>::max(), transport),
        actor_id_(actor_id),
        peer_actor_id_(peer_actor_id),
        queue_id_(queue_id),
        min_consumed_id_(QUEUE_INVALID_SEQ_ID),
        last_recv_seq_id_(QUEUE_INVALID_SEQ_ID),
        last_recv_msg_id_(0),
        expect_seq_id_(1),
        transport_(transport) {}

  /// Delete processed items whose seq id <= seq_id,
  /// then notify upstream queue.
  void OnConsumed(uint64_t seq_id);

  /// Callback function, will be called when PullPeer response comes.
  /// NOTE: this callback function is called in queue thread.
  void OnPullResponse(std::shared_ptr<PullResponseMessage> msg);

  void OnData(QueueItem &item);
  /// Callback function, will be called when PullPeer DATA comes.
  /// TODO: can be combined with OnData
  /// NOTE: this callback function is called in queue thread.
  void OnPullData(std::shared_ptr<PullDataMessage> msg);

  /// PullPeer async version, wait for upstream return.
  bool PullPeerSync(uint64_t seq_id);

  uint64_t GetMinConsumedSeqID() { return min_consumed_id_; }
  void SetMinConsumedSeqId(uint64_t seq_id) { min_consumed_id_ = seq_id; }

  uint64_t GetLastRecvSeqId() { return last_recv_seq_id_; }
  uint64_t GetLastRecvMsgId() { return last_recv_msg_id_; }

  void SetExpectSeqId(uint64_t expect) { expect_seq_id_ = expect; }
 private:
  void Notify(uint64_t seq_id);
  void CreateNotifyTask(uint64_t seq_id, std::vector<TaskArg> &task_args);

 private:
  ActorID actor_id_;
  ActorID peer_actor_id_;
  ObjectID queue_id_;
  uint64_t min_consumed_id_;
  uint64_t last_recv_seq_id_;
  uint64_t last_recv_msg_id_;
  uint64_t expect_seq_id_;
  std::shared_ptr<PromiseWrapper> promise_for_pull_;
  std::shared_ptr<Transport> transport_;
};

}  // namespace streaming
}  // namespace ray
#endif