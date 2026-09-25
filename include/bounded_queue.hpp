#pragma once
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>

namespace event_kafka {

struct QueuedEvent {
  std::string event_id;
  std::string topic;
  std::string msg_key;
  std::string payload;
  std::string call_uuid;
  int64_t enqueued_at_ms{0};
};

class BoundedQueue {
 public:
  explicit BoundedQueue(size_t max_depth) : max_(max_depth) {}

  bool try_push(QueuedEvent ev) {
    std::lock_guard<std::mutex> lk(mu_);
    if (q_.size() >= max_) {
      ++rejected_;
      return false;
    }
    q_.push_back(std::move(ev));
    cv_.notify_one();
    return true;
  }

  std::optional<QueuedEvent> pop_wait_for_ms(int ms, bool& stopping) {
    std::unique_lock<std::mutex> lk(mu_);
    if (!cv_.wait_for(lk, std::chrono::milliseconds(ms),
                      [&] { return stopping || !q_.empty(); })) {
      return std::nullopt;
    }
    if (q_.empty()) return std::nullopt;
    QueuedEvent ev = std::move(q_.front());
    q_.pop_front();
    return ev;
  }

  void notify_stop() { cv_.notify_all(); }

  uint64_t rejected() const {
    std::lock_guard<std::mutex> lk(mu_);
    return rejected_;
  }

  size_t size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return q_.size();
  }

 private:
  size_t max_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::deque<QueuedEvent> q_;
  uint64_t rejected_{0};
};

}  // namespace event_kafka
