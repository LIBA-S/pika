// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_reader_io_thread.h"

PikaReplReaderIOThread::PikaReplReaderIOThread(Transporter* transporter,
                                           const std::set<std::string>& ips,
                                           int port,
                                           int cron_interval) :
  HolyThread(ips, port, &conn_factory_, cron_interval, &handle_, true),
  transporter_(transporter),
  conn_factory_(this),
  port_(port),
  serial_(0) {
  set_keepalive_timeout(180);
}

int PikaReplReaderIOThread::ListenPort() {
  return port_;
}

void PikaReplReaderIOThread::ReplReaderHandle::FdClosedHandle(int fd, const std::string& ip_port) const {
  LOG(INFO) << "ReaderIOThread Close Slave Conn, fd: " << fd << ", ip_port: " << ip_port;
  //g_pika_server->DeleteSlave(fd);
  //g_pika_rm->ReplServerRemoveClientConn(fd);
}

void PikaReplReaderIOThread::ReplReaderConnFactory::AddRemote(const std::string&ip_port, std::shared_ptr<Reader> reader) {
  if (transporter_ == nullptr) {
    return;
  }
  transporter_->AddRemote();
}
