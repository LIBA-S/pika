// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_THREAD_H_
#define PIKA_REPL_SERVER_THREAD_H_

#include "pink/src/holy_thread.h"

#include "include/pika_repl_server_conn.h"

class PikaReplReaderIOThread : public pink::HolyThread {
 public:
  PikaReplReaderIOThread(Transporter* transporter, const std::set<std::string>& ips, int port, int cron_interval);
  virtual ~PikaReplReaderIOThread() = default;

  int ListenPort();

  // for ProcessBinlogData use
  uint64_t GetnPlusSerial() {
    return serial_++;
  }

 private:
  class ReplReaderConnFactory : public pink::ConnFactory {
   public:
    explicit ReplServerConnFactory(PikaReplReaderIOThread* io_thread)
        : io_thread_(io_thread) {
    }

    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string& ip_port,
        pink::Thread* thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const override {
      auto reader = std::make_shared<Reader>(connfd, ip_port, thread, io_thread_, pink_epoll);
      AddRemote(ip_port, reader);
      return std::static_pointer_cast<pink::PinkConn>(reader)
    }
   private:
    void AddRemote(const std::string&ip_port, std::shared_ptr<Reader> reader);

    PikaReplReaderIOThread* io_thread_;
  };

  class ReplReaderHandle : public pink::ServerHandle {
   public:
    virtual void FdClosedHandle(int fd, const std::string& ip_port) const override;
  };

  friend class ReplReaderConnFactory

  Transporter* transporter_;
  ReplReaderConnFactory conn_factory_;
  ReplReaderHandle handle_;
  int port_;
  uint64_t serial_;
};

#endif
