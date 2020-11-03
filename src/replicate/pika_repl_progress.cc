// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pika_repl_progress.h"

namespace replica {

/* SyncWindow */

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
  total_size_ += item.binlog_size_;
}

bool SyncWindow::Update(const SyncWinItem& start_item,
    const SyncWinItem& end_item,  LogOffset* acked_offset) {
  size_t start_pos = win_.size(), end_pos = win_.size();
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (start_pos == win_.size() || end_pos == win_.size()) {
    LOG(WARNING) << "Ack offset Start: " << start_item.ToString()
                 << " End: " << end_item.ToString()
                 << " not found in binlog controller window. window status " << ToStringStatus();
    return false;
  }
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
    total_size_ -= win_[i].binlog_size_;
  }
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.pop_front();
    } else {
      break;
    }
  }
  return true;
}

int SyncWindow::Remaining() {
  std::size_t remaining_size = g_pika_conf->sync_window_size() - win_.size();
  return remaining_size > 0? remaining_size:0 ;
}

/* Progress */

Progress::Progress(const PeerId& peer_id, int session_id)
  : context(peer_id, session_id),
  slave_state(),
  b_state(),
  sent_offset(),
  acked_offset() {
}

Progress::~Progress() {
}

Status Progress::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog,
                                      const BinlogOffset& offset) {
  slash::RWLock l(&slave_mu, true);
  binlog_reader = std::make_shared<PikaBinlogReader>();
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res) {
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

std::string Progress::ToStringStatus() {
  slash::RWLock l(&slave_mu, false);
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << slave_state.ToString() << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << b_state.ToString() << "\r\n";
  tmp_stream << "    Sync_window: " << "\r\n" << sync_win.ToStringStatus();
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";
  return tmp_stream.str();
}

Status Progress::GetInfo(std::stringstream& stream) {
  slash::RWLock l(&slave_mu, false);
  stream << "  replication_status: " << slave_state.ToString() << "\r\n";
  if (slave_state.Code() == SlaveState::Code::kSlaveBinlogSync) {
    BinlogOffset binlog_offset;
    Status s = binlog_reader->GetProducerStatus(&(binlog_offset.filenum), &(binlog_offset.offset));
    if (!s.ok()) {
      return s;
    }
    uint64_t lag = (binlog_offset.filenum - acked_offset.b_offset.filenum) *
                   binlog_reader->BinlogFileSiz() +
                   (binlog_offset.offset - acked_offset.b_offset.offset);
    stream << "  lag: " << lag << "\r\n";
  }
  return Status::OK();
}

Status Progress::Update(const LogOffset& start,
                        const LogOffset& end,
                        LogOffset* updated_offset) {
  slash::RWLock l(&slave_mu, true);
  if (slave_state.Code() != SlaveState::Code::kSlaveBinlogSync) {
    return Status::Corruption(ToString() + "state not BinlogSync");
  }
  *updated_offset = LogOffset();
  bool res = sync_win.Update(SyncWinItem(start), SyncWinItem(end), updated_offset);
  if (!res) {
    return Status::Corruption("UpdateAckedInfo failed");
  }
  if (*updated_offset == LogOffset()) {
    // nothing to update return current acked_offset
    *updated_offset = acked_offset;
    return Status::OK();
  }
  // update acked_offset
  acked_offset = *updated_offset;
  return Status::OK();
}

Status Progress::Initialize(std::shared_ptr<Binglog> logger, const LogOffset& offset) {
  slash::RWLock l(&slave_mu, true);

  slave_state.SetCode(SlaveState::Code::kFollowerBinlogSync);
  sent_offset = offset;
  acked_offset = offset;
  // read binlog file from file
  Status s = InitBinlogFileReader(logger, offset.b_offset);
  if (!s.ok()) {
    return Status::Corruption("Init binlog file reader failed" + s.ToString());
  }
  b_state.SetCode(BinlogSyncState::Code::kReadFromFile);
  return Status::OK();
}

Status Progress::PrepareInfligtBinlog(std::vector<WriteTask>& tasks) {
  slash::RWLock l(&slave_mu, true);

  if (reader == nullptr) {
    return Status::OK();
  }

  int remains = sync_win.Remaining();
  std::vector<WriteTask> tasks;

  for (int i = 0; i < remains; ++i) {
    std::string msg;
    uint32_t filenum;
    uint64_t offset;
    if (sync_win.GetTotalBinlogSize() > PIKA_MAX_CONN_RBUF_HB * 2) {
      LOG(INFO) << ToString() << " total binlog size in sync window is :"
                << sync_win.GetTotalBinlogSize();
      break;
    }
    Status s = reader->Get(&msg, &filenum, &offset);
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      return Status::Corruption("Read Binlog error:" + s.ToString());
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(
          TypeFirst, msg, &item)) {
      return Status::Corruption("Binlog item decode failed");
    }
    BinlogOffset pending_b_offset = BinlogOffset(filenum, offset);
    LogicOffset pending_l_offset = LogicOffset(item.term_id(), item.logic_id());
    LogOffset pending_offset(pending_b_offset, pending_l_offset);

    sync_win.Push(SyncWinItem(pending_offset, msg.size()));
    context.SetLastSendTime(slash::NowMicros());
    WriteTask task;
    task.to = context.peer_id;
    task.builder = std::make_shared<BinlogMessageBuilder>(BinlogChip(pending_offset, msg), sent_offset);
    tasks.push_back(task);
    sent_offset = pending_offset;
  }

  return Status::OK();
}

/* ProgressSet */
ProgressSet::ProgressSet(int consensus_level)
  : consensus_level_(consensus_level) {
  pthread_rwlock_init(&rwlock_, NULL);
}

ProgressSet::~ProgressSet() {
  pthread_rwlock_destroy(&rwlock_);
}

VoteResult ProgressSet::VoteStatus() {
  return kVotePending;
}

Status ProgressSet::IsReady() {
  slash::RWLock l(&rw_lock_, false);
  if (configuration_.voters.size() >= static_cast<int>(consensus_level)) {
    return Status::OK();
  }
  return Status::Incomplete("Not enough follower");
}

int32_t ProgressSet::InsertVoter(const PeerId& peer_id) {
  int32_t session_id;
  std::shared_ptr<Progress> progress;
  {
    slash::RWLock l(&rw_lock_, true);
    auto iter = progress_.find(peer_id);
    if (iter == progress_.end()) {
      session_id = session_id_++;
      progress = std::make_shared<Progress>(peer_id, session_id);
      progress_.insert({peer_id, progress});
      configuration_.voters.insert(peer_id);
    } else {
      progress = iter.second;
    }
  }
  session_id = progress.context.session_id;
  return session_id;
}

int32_t ProgressSet::InsertLearner(const PeerId& peer_id) {
  int32_t session_id;
  std::shared_ptr<Progress> progress;
  {
    slash::RWLock l(&rw_lock_, true);
    auto iter = progress_.find(peer_id);
    if (iter == progress_.end()) {
      session_id = session_id_++;
      progress = std::make_shared<Progress>(peer_id, session_id);
      progress_.insert({peer_id, progress});
      configuration_.learners.insert(peer_id);
    } else {
      progress = iter.second;
    }
  }
  session_id = progress.context.session_id;
  return session_id;
}

Status ProgressSet::Remove(const PeerId& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  auto iter = progress_.find(peer_id);
  if (iter == progress_.end()) {
    return Status::NotFound(peer_id);
  }
  progress_.erase(iter);
  configuration_.learners.erase(peer_id);
  configuration_.voters.erase(peer_id);
  return Status::OK();
}

Status ProgressSet::PromoteLearner(const PeerId& peer_id) {
  slash::RWLock l(&rw_lock_, true);
  if (configuration_.voters.find(peer_id) != configuration_.voters.end()) {
    return Status::Corruption(peer_id.ToString() + " is already a voter");
  }
  auto c_iter = configuration_.learners.find(peer_id);
  if (c_iter == configuration_.learners.end()) {
    return Status::NotFound(peer_id.ToString() + " does not exist in current learners set");
  }
  configuration_.learners.erase(c_iter);
  configuration_.voter.insert(peer_id);
  auto p_iter = progress_.find(peer_id);
  if (p_iter == progress_.end()) {
    return Status::NotFound(peer_id);
  }
  p_iter.second.promote();
  return Status::OK();
}

std::shared_ptr<Progress> Progress(const PeerId& peer_id) {
  slash::RWLock l(&rw_lock_, false);
  auto p_iter = progress_.find(peer_id);
  if (p_iter == progress_.end()) {
    return nullptr;
  }
  return p_iter.second;
}

Status ProgressSet::Update(const PeerId& peer_id,
                           const LogOffset& start,
                           const LogOffset& end,
                           LogOffset* committed_index) {
  auto progress = Progress(peer_id);
  if (progress == nullptr) {
    return Status::NotFound(peer_id.ToString());
  }
  LogOffset acked_offset;
  Status s = progress->Update(start, end, &acked_offset);
  if (!s.ok()) {
    return s;
  }

  *committed_index = InternalCalCommittedIndex();
  return Status::OK();
}

LogOffset ProgressSet::InternalCalCommittedIndex() {
  auto matched_index = GetAllMatchedIndex();
  if (static_cast<int>(matched_index.size()) < consensus_level_) {
    return LogOffset();
  }
  std::sort(matched_index.begin(), matched_index.end());
  LogOffset offset = matched_index[matched_index.size() - consensus_level_];
  return offset;
}

std::vector<LogOffset> ProgressSet::GetAllMatchedIndex() {
  std::vector<LogOffset> matched_index;
  {
    slash::RWLock l(&rwlock_, false);
    for (auto iter : configuration_.voters) {
      auto progress = progress_.find(*iter);
      if (progress_ == nullptr) {
        return matched_index;
      }
      matched_index.push_back({progress.acked_offset});
    }
  }
  return matched_index;
}

Status ProgressSet::GetInfo(std::stringstream& stream) {
  std::unordered_map<PeerID, std::shared_ptr<Progress>> ps;
  {
    slash::RWLock l(&rwlock_, false);
    ps.insert(progress_.begin(), progress_.end());
  }
  stream << "  connected_slaves: " << ps.size() << "\r\n";
  int i = 0;
  for (auto iter : ps) {
    stream << "  slave[" << i++ << "]: ";
    stream << iter.first.ToString() << "\r\n";
    iter.second->GetInfo(stream);
  }
  return Status::OK();
}

} // namespace replica

