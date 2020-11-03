// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_TRANSPORTER_H_
#define PIKA_REPL_TRANSPORTER_H_

class MessageReporter {
 public:
  // After message is processed, the done->Run() is called.
  virtual void HandleRequest(const PeerID& peer_id,
                             InnerMessage::InnerRequest* req,
                             IOClosure* done) = 0;
  virtual void HandleResponse(const PeerID& peer_id,
                              InnerMessage::InnerResponse* res,
                              IOClosure* done) = 0;
  enum ResultType {
    kOK                 = 1;
    kResponseParseError = 2;
  };

  // Report the write result
  virtual void ReportWriteResult(const PeerID& peer_id,
                                 const ResultType& type) = 0;

  // Can not establish connection with peer
  enum UnreachableReason {
    kFdClosed  = 1;
    kFdTimeout = 2;
  };

  virtual void ReportUnreachable(const PeerID& peer_id, const UnreachableReason& reason) = 0;
};

//// Remote for a newly added member to catch up
//class Remote {
// public:
//  Remote(const PeerID& peer_id)
//    : peer_id_(peer_id) { }
//  Remote(const std::string& ip, unsigned short port)
//    : peer_id_(ip, port) { }
//  Remote(const Peer& peer)
//    : peer_id_(peer.peer_id_) { }
// private:
//  PeerID peer_id_;
//
//  StreamWriter writer_;
//  std::shared_ptr<MessageReporter> reporter_;
//};

// Handle the transport to the specific peer addr
class Peer {
 public:
  Peer(const PeerAddr& peer_id)
    : peer_id_(peer_id) { }
  Peer(const std::string& ip, unsigned short port)
    : peer_id_(ip, port) { }
  Peer(const Peer& peer)
    : peer_id_(peer.peer_addr_) { }

  Status Init(std::shared_ptr<MessageReporter> reporter_);

  Status StartReader();

  Status AttachWriter(std::shared_ptr<pink::PbConn> outgoing_conn_);

  Status AppendWriteTask();

  Status SendToPeer(std::queue<WriteTask> tasks);

 private:
  using WriteTaskQueue = std::unordered_map<ReplicationGroupID, std::queue<WriteTask>>>;

  slash::Mutex  write_queue_mu_;
  WriteTaskQueue write_queue_;

  PeerAdd peer_addr_;

  StreamReader reader_;
  StreamWriter writer_;

  std::shared_ptr<MessageReporter> reporter_;
};

class Transporter {
 public:
  Status AddPeer(const std::string& ip, unsigned short port);
  Status AddRemote(const std::string& ip, unsigned short port);
  Status RemovePeer(const std::string& ip, unsigned short port);
  Status RemoveRemote(const std::string& ip, unsigned short port);

  Status Send(const InnerMessage::Message& msg);
  Status ServeClients();

 public:
  void ReportUnreachable(const std::string& ip, unsigned short port,
                         const UnreachableReason& reason) {
    reporter_->ReportUnreachable(PeerID(ip, port), reason);
  }

 private:
  using PeerMap = std::unordered_map<PeerID, std::shared_ptr<Peer>>;

  slash::Mutex map_mu_;
  PeerMap peer_map_;
  ReerMap remote_map_;

  MessageReporter* reporter_;

  PikaReplClientThread* client_thread_;
  PikaReplServerThread* server_thread_;
};

#endif  //  PIKA_REPL_TRANSPORTER_H_
