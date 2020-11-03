// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_APPLY_PROCESSOR_H_
#define PIKA_APPLY_PROCESSOR_H_

#include <vector>
#include <string>

#include "pink/include/bg_thread.h"

class ApplyHandler;

class ApplyTaskArg {
 public:
  ApplyTaskArg(LogOffset _offset, Task _task)
    : offset(std::move(_offset)),
    task(std::move(_task)),
    handler(nullptr) {
  }

  ApplyTaskArg(ApplyTask&& other)
    : offset(std::move(other.offset)),
    task(std::move(other.task),
    handler(std::move(other.handler)) {
    other.handler = nullptr;
  }

  std::string HashStr() {
    return task.group_id.ToString();
  }

  void AttachHandler(ApplyHandler* _handler) {
    handler = _handler;
  }

  void LockApplyHandler() {
    if (handler == nullptr) {
      return;
    }
    handler->SetCurrentTask(this);
  }

  ApplyHandler* Handler() {
    return handler_;
  }

  std::string TableName() {
    return task.group_id.table_name;
  }

  uint32_t PartitionID() {
    return task.group_id.partition_id;
  }

  std::string PeerIpPort() {
    return task.peer_id.ToString();
  }

  const std::string& Log() {
    return task.log;
  }

 private:
  LogOffset offset;
  replica::Task task;
  ApplyHandler* handler;
};

class ApplyTaskArgGuard {
 public:
  ApplyTaskArgGuard(ApplyTaskArg* arg) : arg_(arg) { }
  ~ApplyTaskArgGuard() {
    if (arg_ != nullptr) {
      delete arg_;
      arg_ = nullptr;
    }
  }
  ApplyTaskArg* Release() {
    ApplyTaskArg* current = arg_;
    arg_ = nullptr;
    return current;
  }
 private:
  ApplyTaskArgGuard(const ApplyTaskArgGuard& other);
  ApplyTaskArgGuard& operator=(const ApplyTaskArgGuard& other);

  ApplyTaskArg* arg_;
};

class ApplyHandler {
 public:
  ApplyHandler(size_t max_queue_size, const std::string& name_prefix);

  int Start();

  void Stop();

  void Schedule(ApplyTaskArg* arg) {
    worker_->Schedule(&ApplyHandler::Apply, static_cast<void*>(arg));
  }

 private:
  friend class ApplyTaskArg;

  void SetCurrentTask(ApplyTaskArg* arg) {
    current_task_ = arg;
  }

  static void Apply(void* arg);
  static int DealMesage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
  
  int ApplyFromLog();

 private:
  ApplyTaskArg* current_task_;
  pink::RedisParser redis_parser_;
  pink::BGThread* worker_;
};

class PikaApplyProcessor {
 public:
  PikaApplyProcessor(size_t worker_num,
                     size_t max_quequ_size,
                     const std::string& name_prefix = "ApplyProcessor");
  ~PikaApplyProcessor();
  int Start();
  void Stop();
  void ScheduleApplyTask(ApplyTaskArg* arg);
 private:
  std::vector<ApplyHandler*> apply_handlers_;
};

#endif // PIKA_APPLY_PROCESSOR_H_
