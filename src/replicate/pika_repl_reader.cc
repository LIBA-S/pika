// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"

class ReaderClosure : public IOClosure {
 public:
  ReaderClosure(std::shared_ptr<Reader> reader)
    : reader_(reader) { }

  virtual void Run() override {
    run();
    delete this;
  }

 private:
  void run();

  std::shared_ptr<Reader> reader_;
};

void ReaderClosure::run() {
  if (reader_ == nullptr) {
    return;
  }
  if (should_response_ && !reader_->WriteResp(resp_str_)) {
    reader_->NotifyClose();
    return;
  }
  reader_->NotifyWrite();
}

Reader::Reader(int fd, std::string ip_port,
              pink::Thread* thread,
              void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

Reader::~Reader() {
}

void Reader::SetMessageReporter(std::shared_ptr<MessageReporter> reporter) {
  reporter_ = reporter;
}

int Reader::DealMessage() {
  InnerMessage::InnerRequest* req = new InnerMessage::InnerRequest();
  RequestGuard guard(req);
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  if (reporter_ == nullptr) {
    LOG(WARNING) << "Pika repl server connection do not register message reporter.";
    return -1;
  }
  ReaderClosure* c = new ReaderClosure(std::shared_from_this());
  reporter_->HandleRequest(PeerID(ip_port()), guard.Release(), c);
  return 0;
}
