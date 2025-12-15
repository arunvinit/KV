#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <unordered_map>
#include <optional>
#include <string>
 
// ==================== Request ====================
 
enum class OpType {
    GET,
    SET,
    DEL
};
 
struct Request {
    OpType type;
    std::string key;
    std::string value; // only for SET
};
 
// ==================== BlockingQueue ====================
 
template <typename T>
class BlockingQueue {
public:
    void push(T item) {
        {
            std::lock_guard<std::mutex> lock(m);
            q.push(std::move(item));
        }
        cv.notify_one();
    }
 
    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock, [&] { return stopped || !q.empty(); });
 
        if (stopped && q.empty())
            return false;
 
        item = std::move(q.front());
        q.pop();
        return true;
    }
 
    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(m);
            stopped = true;
        }
        cv.notify_all();
    }
 
private:
    std::queue<T> q;
    std::mutex m;
    std::condition_variable cv;
    bool stopped = false;
};
 
// ==================== Shard ====================
 
class Shard {
public:
    std::optional<std::string> get(const std::string& key) {
        std::shared_lock<std::shared_mutex> lock(m);
        auto it = data.find(key);
        if (it == data.end())
            return std::nullopt;
        return it->second;
    }
 
    void set(const std::string& key, const std::string& value) {
        std::unique_lock<std::shared_mutex> lock(m);
        data[key] = value;
    }
 
    void del(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(m);
        data.erase(key);
    }
 
private:
    std::unordered_map<std::string, std::string> data;
    std::shared_mutex m;
};
 
// ==================== KVStore ====================
 
class KVStore {
public:
    KVStore(size_t shardCount) : shards(shardCount) {}
 
    std::optional<std::string> get(const std::string& key) {
        return shard(key).get(key);
    }
 
    void set(const std::string& key, const std::string& value) {
        shard(key).set(key, value);
    }
 
    void del(const std::string& key) {
        shard(key).del(key);
    }
 
private:
    std::vector<Shard> shards;
 
    Shard& shard(const std::string& key) {
        size_t index = std::hash<std::string>{}(key) % shards.size();
        return shards[index];
    }
};
 
// ==================== ThreadPool ====================
 
class ThreadPool {
public:
    ThreadPool(size_t numThreads, KVStore& store)
        : kv(store) {
        for (size_t i = 0; i < numThreads; ++i) {
            workers.emplace_back(&ThreadPool::workerLoop, this, i);
        }
    }
 
    void submit(Request req) {
        queue.push(std::move(req));
    }
 
    void shutdown() {
        queue.shutdown();
        for (auto& t : workers)
            t.join();
    }
 
private:
    void workerLoop(int id) {
        Request req;
        while (queue.pop(req)) {
            switch (req.type) {
                case OpType::GET: {
                    auto val = kv.get(req.key);
                    if (val) {
                        std::cout << "[Worker " << id << "] GET "
                                  << req.key << " = " << *val << "\n";
                    } else {
                        std::cout << "[Worker " << id << "] GET "
                                  << req.key << " = <null>\n";
                    }
                    break;
                }
                case OpType::SET:
                    kv.set(req.key, req.value);
                    std::cout << "[Worker " << id << "] SET "
                              << req.key << "\n";
                    break;
 
                case OpType::DEL:
                    kv.del(req.key);
                    std::cout << "[Worker " << id << "] DEL "
                              << req.key << "\n";
                    break;
            }
        }
    }
 
    BlockingQueue<Request> queue;
    std::vector<std::thread> workers;
    KVStore& kv;
};
 
// ==================== main ====================
 
int main() {
    const size_t NUM_SHARDS = 4;
    const size_t NUM_WORKERS = 2;
 
    KVStore store(NUM_SHARDS);
    ThreadPool pool(NUM_WORKERS, store);
 
    // Produce requests
    pool.submit({OpType::SET, "a", "1"});
    pool.submit({OpType::SET, "b", "2"});
    pool.submit({OpType::GET, "a", ""});
    pool.submit({OpType::DEL, "a", ""});
    pool.submit({OpType::GET, "a", ""});
    pool.submit({OpType::GET, "b", ""});
 
    pool.shutdown();
 
    std::cout << "KV Store shutdown cleanly\n";
    return 0;
}