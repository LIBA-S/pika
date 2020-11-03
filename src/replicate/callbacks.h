// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CALLBACKS_H_
#define PIKA_CALLBACKS_H_

namespace replica {

class Closure {
 public:
  Closure() { }
  virtual ~Closure() { }

  virtual void Run() = 0;

 private:
  Closure(const Closure& c);
  void operator =(const Closure& c);
};

class IOClosure : public Closure {
 public:
  IOClosure()
  : should_response_(false),
  should_close_(false),
  resp_str_("") { }
  virtual ~Closure() { }

  virtual void Run() override { }

  void SetResponse(std::string&& str, bool should_response) {
    resp_str_ = str;
    should_response_ = should_response;
  }

  void NotifyClose() {
    should_close_ = true;
  }

 private:
  Closure(const Closure& c);
  void operator =(const Closure& c);

  bool should_response_;
  bool should_close_;
  std::string resp_str_;
};

// RAII of Closure
class ClosureGuard {
 public:
  ClosureGuard() : closure_(nullptr) { }
  explicit ClosureGuard(Closure* closure) : closure_(closure) { }

  ~ClosureGuard() {
    if (closure_ != nullptr) {
      closure_->Run();
      delete closure_;
    }
  }

  void Reset(Closure* closure) {
    if (closure_ != nullptr) {
      closure_->Run();
    }
    closure_ = closure;
  }

  Closure* Release() {
    Closure* cur_closure = closure_;
    closure_ = nullptr;
    return cur_closure;
  }

 private:
  ClosureGuard(const ClosureGuard& cg);
  void operator =(const ClosureGuard& cg);

  Closure* closure_;
};

// RAII of Response
class ResponseGuard {
 public:
  ResponseGuard() : response_(nullptr) { }
  explicit ResponseGuard(InnerMessage::InnerResponse* response) : response_(response) { }

  ~ResponseGuard() {
    if (response_ != nullptr) {
      delete response_;
    }
  }

  void Reset(Response* Response) {
    if (response_ != nullptr) {
      delete response_;
    }
    response_ = Response;
  }

  InnerMessage::InnerResponse* Release() {
    InnerMessage::InnerResponse* cur_response = response_;
    response_ = nullptr;
    return cur_response;
  }

 private:
  ResponseGuard(const ResponseGuard& cg);
  void operator =(const ResponseGuard& cg);

  InnerMessage::InnerResponse* response_;
};

// RAII of Request
class RequestGuard {
 public:
  RequestGuard() : request_(nullptr) { }
  explicit RequestGuard(InnerMessage::InnerRequest* request) : request_(request) { }

  ~RequestGuard() {
    if (request_ != nullptr) {
      delete request_;
    }
  }

  void Reset(Request* request) {
    if (request_ != nullptr) {
      delete request_;
    }
    request_ = request;
  }

  InnerMessage::InnerRequest* Release() {
    InnerMessage::InnerRequest* cur_request = request_;
    request_ = nullptr;
    return cur_request;
  }

 private:
  RequestGuard(const RequestGuard& cg);
  void operator =(const RequestGuard& cg);

  InnerMessage::InnerRequest* request_;
};

} // namespace replica

#endif // PIKA_CALLBACKS_H_
