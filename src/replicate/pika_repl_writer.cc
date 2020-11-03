// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"

class WriterClosure : public IOClosure {
 public:
  WriterClosure(std::shared_ptr<Writer> Writer)
    : Writer_(Writer) { }

  virtual void Run() override {
    run();
    delete this;
  }

 private:
  void run();

  std::shared_ptr<Writer> Writer_;
};

void WriterClosure::run() {
  if (Writer_ == nullptr) {
    return;
  }
  if (should_close_) {
    Writer_->NotifyClose();
    return;
  }
}

Writer::Writer(int fd, std::string ip_port,
              pink::Thread* thread,
              void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

Writer::~Writer() {
}

void Writer::SetMessageReporter(std::shared_ptr<MessageReporter> reporter) {
  reporter_ = reporter;
}

int Writer::DealMessage() {
  InnerMessage::InnerResponse* response = new InnerMessage::InnerResponse();
  ResponseGuard guard(response);

  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, header_len_);
  ::google::protobuf::io::CodedInputStream decoder(&input);
  decoder.SetTotalBytesLimit(g_pika_conf->max_conn_rbuf_size(), g_pika_conf->max_conn_rbuf_size());
  bool success = response->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (reporter_ == nullptr) {
    LOG(WARNING) << "MessageReporter is nullptr ";
    return -1;
  }
  if (!success) {
    LOG(WARNING) << "ParseFromArray FAILED! " << " msg_len: " << header_len_;
    reporter_->ReportWriteResult(PeerID(ip_port),
                                 MessageReporter::ResultType::kResponseParseError);
    return -1;
  }
  WriterClosure* c = new WriterClosure(std::shared_from_this());
  reporter_->HandleResponse(PeerID(ip_port()), guard.Release(), c);
  return 0;
}
