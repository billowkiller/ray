#ifndef RAY_RPC_SERVER_CALL_H
#define RAY_RPC_SERVER_CALL_H

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"

namespace ray {

using RequestDoneCallback = std::function<void(Status)>;

enum class ServerCallState { PENDING, PROCECCSSING, REPLY_SENT };

class UntypedServerCallFactory;

class UntypedServerCall {
 public:
  virtual ServerCallState GetState() const = 0;
  virtual void Proceed() = 0;
  virtual const UntypedServerCallFactory &GetFactory() const = 0;
};

class UntypedServerCallFactory {
 public:
  virtual UntypedServerCall *CreateCall() const = 0;
};

template <class GcsService, class ServiceHandler, class Request, class Reply>
class ServerCallFactory;

template <class ServiceHandler, class Request, class Reply>
class ServerCall : public UntypedServerCall {
  // Represents the generic signature of a `Service::HandleFoo()`
  // method, where `Foo` is the name of an RPC method.
  using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                         RequestDoneCallback);

 public:
  ServerCall(const UntypedServerCallFactory &factory, ServiceHandler *service_handler,
             HandleRequestFunction handle_request_function)
      : state_(ServerCallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        response_writer_(&context_) {}

  ServerCallState GetState() const override { return state_; }

  void Proceed() override {
    if (state_ == ServerCallState::PENDING) {
      state_ = ServerCallState::PROCECCSSING;
      (service_handler_->*handle_request_function_)(
          request_, &reply_, [this](Status status) { SendResponse(status); });
    } else if (state_ == ServerCallState::PROCECCSSING) {
      state_ = ServerCallState::REPLY_SENT;
    }
  }

  const UntypedServerCallFactory &GetFactory() const override { return factory_; }

 private:
  void SendResponse(Status status) {
    // TODO
    ::grpc::Status grpc_status = ::grpc::Status::OK;
    response_writer_.Finish(reply_, grpc_status, this);
  }

  ServerCallState state_;

  const UntypedServerCallFactory &factory_;

  ServiceHandler *service_handler_;
  HandleRequestFunction handle_request_function_;

  ::grpc::ServerContext context_;
  ::grpc::ServerAsyncResponseWriter<Reply> response_writer_;

  Request request_;
  Reply reply_;

  template <class GcsServiceX, class ServiceHandlerX, class RequestX, class ReplyX>
  friend class ServerCallFactory;
};

template <class GcsService, class ServiceHandler, class Request, class Reply>
class ServerCallFactory : public UntypedServerCallFactory {
  using AsyncService = typename GcsService::AsyncService;

  using RequestCallFunction = void (AsyncService::*)(
      ::grpc::ServerContext *, Request *, ::grpc::ServerAsyncResponseWriter<Reply> *,
      ::grpc::CompletionQueue *, ::grpc::ServerCompletionQueue *, void *);

  using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                         RequestDoneCallback);

 public:
  ServerCallFactory(AsyncService *service, RequestCallFunction request_call_function,
                    ServiceHandler *service_handler,
                    HandleRequestFunction handle_request_function,
                    const std::unique_ptr<::grpc::ServerCompletionQueue> &cq)
      : service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq) {}

  UntypedServerCall *CreateCall() const override {
    auto call = new ServerCall<ServiceHandler, Request, Reply>(*this, service_handler_,
                                                               handle_request_function_);
    (service_->*request_call_function_)(&call->context_, &call->request_,
                                        &call->response_writer_, cq_.get(), cq_.get(),
                                        call);
    return call;
  }

 private:
  AsyncService *service_;
  RequestCallFunction request_call_function_;
  ServiceHandler *service_handler_;
  HandleRequestFunction handle_request_function_;
  const std::unique_ptr<::grpc::ServerCompletionQueue> &cq_;
};

}  // namespace ray

#endif
