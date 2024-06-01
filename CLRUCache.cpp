#include <string>
#include <any>
#include <unordered_map>
#include <mutex>
#include <vector>
#include <memory>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <functional>
#include <thread>
#include <iostream>

class Node {
public:
	std::string key;
	std::any value;
	Node *prev, *next;
	Node(const std::string& k = "", std::any v = nullptr): 
		key(k), value(v), prev(nullptr), next(nullptr) { }

};

class Bucket {
private:
	std::unordered_map<std::string, Node*> keys;
	std::mutex mtx;

public:
	Node* get(const std::string& key) {
		std::lock_guard<std::mutex> lock(mtx);
		auto it = keys.find(key);
		if (it == keys.end()) {
			return nullptr;
		}
		return it->second;
	}

	Node* set(const std::string& key, std::any value) {
		std::lock_guard<std::mutex> lock(mtx);
		auto it = keys.find(key);
		if (it != keys.end()) {
			auto node = it->second;
			node->value = value;
			return node;
		}
		auto node = new Node(key, value);
		keys[key] = node;
		return node;
	}

	void update(const std::string& key, Node* node) {
		std::lock_guard<std::mutex> lock(mtx);
		keys[key] = node;
	}

	void del(const std::string& key) {
		std::lock_guard<std::mutex> lock(mtx);
		keys.erase(key);
	}

	void clear() {
		std::lock_guard<std::mutex> lock(mtx);
		keys.clear();
	}

};


class CLRUCache {
private:
	int cap;
	Node* dummy;
	std::vector<std::shared_ptr<Bucket>> buckets;
	uint32_t bucketMask;
	std::atomic<bool> stop_flag;
	std::mutex mtx;
	std::condition_variable cv;
	std::thread worker_thread;
	std::queue<std::function<void()>> tasks;
	std::mutex task_mtx;
	std::condition_variable task_cv;

	void remove(Node *x) {
		if (x && x->prev && x->next) {
			x->next->prev = x->prev;
			x->prev->next = x->next;
		}
	}

	void pushToFront(Node* node) {
		if (node) {
			node->next = dummy->next;
			node->prev = dummy;
			node->prev->next = node;
			node->next->prev = node;
		}
	}

	void worker() {
		while (!stop_flag || !tasks.empty()) {
			std::function<void()> task;
			{
				std::unique_lock<std::mutex> lock(task_mtx);
				task_cv.wait(lock, [&] {return !tasks.empty() || stop_flag;});
				if (stop_flag && tasks.empty()) break;
				task = std::move(tasks.front());
				tasks.pop();
			}
			task();
		}
	}

	std::shared_ptr<Bucket> getBucket(const std::string& key) {
		std::hash<std::string> hash_fn;
		return buckets[hash_fn(key) & bucketMask];
	}

public:
	CLRUCache(int capacity) : cap(capacity), bucketMask(1024 - 1), stop_flag(false) {
		dummy = new Node();
		dummy->next = dummy;
		dummy->prev = dummy;
		for (int i = 0; i < 1024; ++i) {
			buckets.push_back(std::make_shared<Bucket>());
		}
		worker_thread = std::thread(&CLRUCache::worker, this);
	}

	~CLRUCache() {
		stop_flag = true;
		task_cv.notify_all();
		if (worker_thread.joinable()) {
			worker_thread.join();
		}

		Node *cur = dummy->next;
		while (cur != dummy) {
			auto next = cur->next;
			delete cur;
			cur = next;
		}
		delete dummy;

	}

	void moveToFront(Node *node) {
		std::lock_guard<std::mutex> lock(mtx);
		remove(node);
		pushToFront(node);
	}

	void async(std::function<void()> task) {
		{
			std::lock_guard<std::mutex> lock(task_mtx);
			tasks.push(std::move(task));
		}
		task_cv.notify_one();
	}


	std::any get(const std::string& key) {
		auto bucket = getBucket(key);
		Node *node = bucket->get(key);
		if (node) {
			async([this, node] {moveToFront(node); });
			return node->value;
		}
		return nullptr;
	}

	int size() {
		int cnt = 0;
		auto cur = dummy->next;
		while (cur != dummy) {
			cur = cur->next;
			cnt++;
		}
		return cnt;
	}

	void put(const std::string& key, std::any value) {
		auto bucket = getBucket(key);
		auto node = bucket->set(key, value);
		async([this, node] {moveToFront(node); });

		async([this, bucket] {
			if (size() > cap) {
				Node* last = dummy->prev;
				if (last != dummy) {
					remove(last);
					bucket->del(last->key);
					delete last;
				}
			}
		});
	}

	void del(const std::string& key) {
		auto bucket = getBucket(key);
		auto node = bucket->get(key);
		if (node) {
			async([this, node] {remove(node); delete node;});
			bucket->del(key);
		}
	}

	void clear() {
		std::unique_lock<std::mutex> lock(mtx);
		for (auto &bucket : buckets) {
			bucket->clear();
		}

		auto cur = dummy->next;
		while (cur != nullptr) {
			auto next = cur->next;
			delete cur;
			cur = next;
		}
		dummy->next = dummy;
		dummy->prev = dummy;
		cv.notify_all();
	}

};


int main() {
	CLRUCache cache(10);

	cache.async([&cache] {cache.put("test", 27);});
	cache.async([&cache] {
		auto value = std::any_cast<int> (cache.get("test"));
		std::cout << "Value: " << value << std::endl;
	});

	std::this_thread::sleep_for(
		std::chrono::seconds(1)
	);

	return 0;
}