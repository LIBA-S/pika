// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_MESSAGE_H_
#define PIKA_REPL_MESSAGE_H_

namespace replica {

class MessageBuilder {
 public:
  MessageBuilder();
  virtual ~MessageBuilder() { };

  virtual std::string Build() = 0;
};

class SimpleMessageBuilder
  : public MessageBuilder,
    public std::enable_shared_from_this<SimpleMessageBuilder> {
 public:
  SimpleMessageBuilder(const std::string& str) : msg_(str) { }
  SimpleMessageBuilder(std::string&& str) : msg_(std::move(str)) { }

  virtual std::string Build() override {
    return std::move(msg_);
  }

 private:
  std::string msg_;
};

struct BinlogChip {
  LogOffset prev_offset_;
  LogOffset pending_offset_;
  std::string binlog_;

  BinlogChip(LogOffset prev_offset, LogOffset pending_offset, std::string binlog)
    : prev_offset_(prev_offset),
    pending_offset_(pending_offset),
    binlog_(binlog) { }

  BinlogChip(const BinlogChip& binlog_chip) {
    offset_ = binlog_chip.offset_;
    binlog_ = binlog_chip.binlog_;
  }

  BinlogChip(BinlogChip&& other)
    : prev_offset_(std::move(other.prev_offset_)),
    : pending_offset_(std::move(other.pending_offset_)),
    binlog_(std::move(other.binlog_)) { }
};

struct WriteTask {
  enum Type {
    kNone      = 1;
    kBinlogMsg = 2;
    kOtherMsg  = 3;
  };
  Type type;
  PeerID from;
  PeerID to;
  ReplicationGroupID rg_id;
  uint32_t session_id;
  struct BinlogChip binlog_chip;
  std::shared_ptr<MessageBuilder> builder;

  WriteTask(PeerID _from, PeerID _to, ReplicationGroupID _rg_id)
    : Type(kNone),
    from(std::move(_from)),
    to(std::move(_to)),
    rg_id(std::move(_rg_id)) { }
};

} // namespace replica

#endif // PIKA_REPL_MESSAGE_H_
