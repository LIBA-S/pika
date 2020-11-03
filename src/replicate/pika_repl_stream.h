// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_STREAM_H_
#define PIKA_REPL_STREAM_H_

/* 
 * StreamReader receive the proactive messages from peer through the connection dialed by peer.
 *
 * ServerSide
 *
 * The messages including:
 *
 *  MetaSyncRequest
 *  DBSyncRequest
 *  TrySyncRequest
 *
 */
class Reader: public pink::PbConn {
 public:
  Reader(int fd,
      std::string ip_port,
      pink::Thread* io_thread,
      void* worker_specific_data,
      pink::PinkEpoll* epoll);

  void SetMessageReporter(std::shared_ptr<MessageReporter> reporter);

  int DealMessage() override;

 private:
  shared_ptr<MessageReporter> reporter_;
};

class StreamReader {
 public:
  Status AttachReader(std::shared_ptr<pink::PbConn> conn);
 private:
  std::shared_ptr<Reader> reader_;
};

/* 
 * StreamWriter post the local messages to peer through the connection dialed by the itself.
 * and handle the responses.
 * ClientSide
 *
 * The messages including:
 *
 *  MetaSyncRequest
 *  DBSyncRequest
 *  TrySyncRequest
 * 
 *  Non-thread-safe
 */ 
class Writer : public pink::PbConn {
 public:
  int DealMessage() override;
  void SetMessageReporter(std::shared_ptr<MessageReporter> reporter);
 private:
  std::shared_ptr<MessageReporter> reporter_;
};

class StreamWriter {
 public:
  void SetMessageReporter(std::shared_ptr<MessageReporter> reporter);
 private:
  std::shared_ptr<Writer> writer_;
};

#endif // PIKA_REPL_STREAM_H_
