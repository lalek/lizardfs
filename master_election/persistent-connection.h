#ifndef PERSISTENT_CONNECTION_H_
#define PERSISTENT_CONNECTION_H_

#include <condition_variable>
#include <mutex>
#include <string>

class PersistentConnection {
 public:
  PersistentConnection(const std::string& hostname, int port);

  // This method blocks, which is not a very nice design. This class will likely
  // be thrown anyway before we need async messages. Timeout for reply would
  // also be nice.
  bool SendMessage(const void* data, size_t n,
                   void* result_data, size_t result_size);

  // Blocks, should be executed in dedicated thread.
  void Run();

 private:
  void SocketFailed();
  bool IsConnected() const { return socket_ >= 0; }

  const std::string hostname_;
  const int port_;

  int socket_;
  std::mutex mu_;
  std::condition_variable socket_fail_cond_;
};

#endif /* PERSISTENT_CONNECTION_H_ */
