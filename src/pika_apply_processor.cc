// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_apply_processor.h"

#include <glog/logging.h>

#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

PikaApplyProcessor::PikaApplyProcessor(
    size_t worker_num, size_t max_queue_size, const std::string& name_prefix) {
  for (size_t i = 0; i < worker_num; ++i) {
    ApplyHandler* apply_handler = new ApplyHandler(max_queue_size, name_prefix);
    apply_handlers_.push_bak(apply_handler);
  }
}

PikaApplyProcessor::~PikaApplyProcessor() {
  for (size_t i = 0; i < apply_hanlders_.size(); ++i) {
    delete apply_hanlders_[i];
  }
  LOG(INFO) << "PikaApplyProcessor exit!!!";
}

int PikaApplyProcessor::Start() {
  for (size_t i = 0; i < apply_hanlders_.size(); ++i) {
    res = apply_hanlders_[i]->Start();
    if (res != pink::kSuccess) {
      return res;
    }
  }
  return res;
}

void PikaApplyProcessor::Stop() {
  for (size_t i = 0; i < apply_hanlders_.size(); ++i) {
    bg_threads_[i]->Stop();
  }
}

void PikaApplyProcessor::ScheduleApplyTask(ApplyHandler::ApplyTaskArg* apply_task_arg) {
  std::size_t index =
    std::hash<std::string>{}(apply_task_arg.HashStr()) % apply_handlers_.size();
  apply_task_arg->AttachHandler(apply_handlers_[index]);
  apply_handlers_[index]->Schedule(apply_task_arg);
}

ApplyHandler::ApplyHandler(size_t max_queue_size, const std::string& name_prefix) {
  pink::BGThread* worker = new pink::BGThread(max_queue_size);
  worker->set_thread_name(name_prefix + "worker");
  pink::RedisParserSettings settings;
  settings.DealMessage = &(ApplyHandler::DealMessage);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

ApplyHandler::~ApplyHandler() {
  if (worker_ != nullptr) {
    delete worker_;
    worker_ = nullptr;
  }
}

int ApplyHandler::Start() {
  return worker_->StartThread();
}

void ApplyHandler::Stop() {
  worker_->StopThread();
}

void ApplyHandler::Apply(void* arg) {
  ApplyTaskArg* task_arg = static_cast<ApplyTaskArg*>(arg);
  ApplyTaskArgGuard task_guard(task_arg);
  task_arg->LockApplyHandler();
  const replica::Task& task = task_arg.task;
  replica::Closure* done = task.done;
  switch task.type {
    case replica::Task::Type::kClientType: {
      replica::ClosureGuard done_guard(done);
      if (done == nullptr) {
        LOG(WARNING) << "Unexpected client requests";
        break;
      }
      LogClosure* closure = static_cast<LogClosure*>(done);
      done->SetLogOffset(task_arg.offset);
    }
    case replica::Task::Type::kRedoType: 
    case replica::Task::Type::kReplicaType: {
      if (done == nullptr && task.log.empty()) {
        LOG(WARNING) << "Unexpected redo requests";
        continue;
      }
      ApplyHandler* handler = task_arg->Handler();
      handler_->ApplyFromLog();
      break;
    }
  }
}

int ApplyHandler::ApplyFromLog() {
  // Parse log to comand and execute it.
  const std::string& log = current_task_->Log();
  const char* redis_parser_start = log.data() + BINLOG_ENCODE_LEN;
  int redis_parser_len = static_cast<int>(log.size()) - BINLOG_ENCODE_LEN;
  int processed_len = 0;
  pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(redis_parser_start,
                                                                 redis_parser_len,
                                                                 &processed_len);
  // We should advance applied index anyway.
  std::string table_name = current_task_->TableName();
  uint32_t partition_id = current_task_->PartitionID();
  LogOffset offset = current_task_->offset;

  std::shared_ptr<ReplicationGroupNode> node =
    g_pika_server->pika_rm_->GetReplicationGroupNode(ReplicationGroupID(table_name, partition_id));
  if (node == nullptr) {
    LOG(WARNING) << "Sync Master Partition not exist " << table_name << partition_id;
    return -1;
  }
  node->Advance(offset);

  if (ret != pink::kRedisParserDone) {
    LOG(WARNING) << "ApplyFromLog parse redis failed";
    return -1;
  }
  return 0;
}

int ApplyHandler::DealMesage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  std::string opt = argv[0];
  ApplyHandler* handler = static_cast<ApplyHandler*>(parser->data);
  ApplyTaskArg* current_task = handler->current_task_;
  std::string table_name = current_task->TableName();
  uint32_t partition_id = current_task->PartitionID();

  // Monitor related
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0 * slash::NowMicros() / 1000000)
      + " [" + table_name + " " + current_task->PeerIpPort() + "]";
    for (const auto& item : argv) {
      monitor_message += " " + slash::ToRead(item);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
  if (!c_ptr) {
    LOG(WARNING) << "Command " << opt << " not in the command table";
    return -1;
  }

  // Initialize command
  c_ptr->Initial(argv, table_name);
  if (!c_ptr->res().ok()) {
    LOG(WARNING) << "Fail to initial command from binlog: " << opt;
    return -1;
  }

  g_pika_server->UpdateQueryNumAndExecCountTable(table_name, opt, c_ptr->is_write());

  // Applied to backend DB
  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  // Add read lock for no suspend command
  if (!c_ptr->is_suspend()) {
    partition->DbRWLockReader();
  }

  c_ptr->Do(partition);

  if (!c_ptr->is_suspend()) {
    partition->DbRWUnLock();
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int32_t start_time = start_us / 1000000;
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      g_pika_server->SlowlogPushEntry(argv, start_time, duration);
      if (g_pika_conf->slowlog_write_errorlog()) {
        LOG(ERROR) << "command: " << argv[0] << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }
}


void ApplyHandler::ApplyClient(ApplyTaskArg* arg) {

}

void ApplyHandler::ApplyRedo(ApplyTaskArg* arg) {

}

void ApplyHandler::ApplyReplica(ApplyTaskArg* arg) {

}
