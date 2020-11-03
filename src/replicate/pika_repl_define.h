// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_DEFINE_H_
#define PIKA_REPL_DEFINE_H_

/* replication Log offset*/

struct LogicOffset {
  uint32_t term;
  uint64_t index;
  LogicOffset()
    : term(0), index(0) {}
  LogicOffset(uint32_t _term, uint64_t _index)
    : term(_term), index(_index) {}
  LogicOffset(const LogicOffset& other) {
    term = other.term;
    index = other.index;
  }
  LogicOffset(LogicOffset&& other)
    : term(std::move(other.term)),
    index(std::move(other.index)) { }
  bool operator==(const LogicOffset& other) const {
    return term == other.term && index == other.index;
  }
  bool operator!=(const LogicOffset& other) const {
    return term != other.term || index != other.index;
  }

  std::string ToString() const {
    return "term: " + std::to_string(term) + " index: " + std::to_string(index);
  }
};

struct BinlogOffset {
  uint32_t filenum;
  uint64_t offset;
  BinlogOffset()
      : filenum(0), offset(0) {}
  BinlogOffset(uint32_t num, uint64_t off)
      : filenum(num), offset(off) {}
  BinlogOffset(const BinlogOffset& other) {
    filenum = other.filenum;
    offset = other.offset;
  }
  BinlogOffset(BinlogOffset&& other)
    : filenum(std::move(other.filenum)),
    offset(std::move(other.offset)) { }
  std::string ToString() const {
    return "filenum: " + std::to_string(filenum) + " offset: " + std::to_string(offset);
  }
  bool operator==(const BinlogOffset& other) const {
    if (filenum == other.filenum && offset == other.offset) {
      return true;
    }
    return false;
  }
  bool operator!=(const BinlogOffset& other) const {
    if (filenum != other.filenum || offset != other.offset) {
      return true;
    }
    return false;
  }

  bool operator>(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset > other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset < other.offset)) {
      return true;
    }
    return false;
  }
  bool operator<=(const BinlogOffset& other) const {
    if (filenum < other.filenum
        || (filenum == other.filenum && offset <= other.offset)) {
      return true;
    }
    return false;
  }
  bool operator>=(const BinlogOffset& other) const {
    if (filenum > other.filenum
        || (filenum == other.filenum && offset >= other.offset)) {
      return true;
    }
    return false;
  }
};

struct LogOffset {
  LogOffset(const LogOffset& _log_offset) {
    b_offset = _log_offset.b_offset;
    l_offset = _log_offset.l_offset;
  }
  LogOffset(LogOffset&& offset)
    : b_offset(std::move(offset.b_offset)),
    l_offset(std::move(offset.l_offset)) { }
  LogOffset() : b_offset(), l_offset() {
  }
  LogOffset(BinlogOffset _b_offset, LogicOffset _l_offset)
    : b_offset(_b_offset), l_offset(_l_offset) {
  }
  bool operator<(const LogOffset& other) const {
    return b_offset < other.b_offset;
  }
  bool operator==(const LogOffset& other) const {
    return b_offset == other.b_offset;
  }
  bool operator<=(const LogOffset& other) const {
    return b_offset <= other.b_offset;
  }
  bool operator>=(const LogOffset& other) const {
    return b_offset >= other.b_offset;
  }
  bool operator>(const LogOffset& other) const {
    return b_offset > other.b_offset;
  }
  std::string ToString() const  {
    return b_offset.ToString() + " " + l_offset.ToString();
  }
  BinlogOffset b_offset;
  LogicOffset  l_offset;
};

/* replication IDs */

// ReplicationGroupID specific a replication group
struct ReplicationGroupID {
  std::string table_name;
  uint32_t partition_id;

  ReplicationGroupID(const std::string& _table_name, uint32_t _partition_id)
    : table_name(_table_name), partition_id(_partition_id) {
  }
  ReplicationGroupID() : partition_id_(0) {
  }
  bool operator==(const ReplicationGroupID& other) const {
    if (table_name == other.table_name
      && partition_id == other.partition_id) {
      return true;
    }
    return false;
  }
  int operator<(const ReplicationGroupID& other) const {
    int ret = strcmp(table_name.data(), other.table_name.data());
    if (!ret) {
      if (partition_id < other.partition_id) {
        ret = -1;
      } else if (partition_id > other.partition_id) {
        ret = 1;
      } else {
        ret = 0;
      }
    }
    return ret;
  }
  std::string ToString() const {
    return "(" + table_name + ":" + std::to_string(partition_id) + ")";
  }
};

struct hash_replication_group_id {
  size_t operator()(const ReplicationGroupID& cid) const {
    return std::hash<std::string>()(cid.table_name_) ^ std::hash<uint32_t>()(cid.partition_id_);
  }
};

// NodeID specific a replica of replication group
struct NodeID {
  PeerID peer_id;
  ReplicationGroupID replication_group_id;
};

// PeerID specific the peer addr of a replica
class PeerID {
 public:
  Peer()
    : ip(""), port(0) {}
  PeerID(const std::string& ip, unsigned short port)
    : ip_(ip), port_(port) { }
  PeerID(const PeerID& peer_id)
    : ip_(peer_id.ip_), port_(peer_id.port_) { }

  void SetIp(const std::string& ip) {
    ip_ = ip;
  }

  const std::string& Ip() const {
    return ip_;
  }
  unsigned short Port() const {
    return port_;
  }
  std::string ToString() const {
    return ip_ + ":" + std::to_string(port_);
  }

 private:
  std::string ip_;
  unsigned short port_;
};

#endif // namespace PIKA_REPL_DEFINE_H_
