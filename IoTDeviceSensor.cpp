#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <thread>
#include <chrono>
#include <unordered_map>
#include <unordered_set>
#include <format>
#include <cstdlib>

struct SensorData {
    int sensorId;
    long eventId;
    double temperature;
    long timestamp;
};

template < typename T >
    class ThreadSafeQueue {
        std::mutex mtx;
        std::queue < T > q;
        int capacity;
        std::condition_variable notfull;
        std::condition_variable notempty;
        bool shutdownflag = false;

        public:
            explicit ThreadSafeQueue(int cap): capacity(cap) {}

        bool push(const T & item) {
            std::unique_lock < std::mutex > lock(mtx);
            notfull.wait(lock, [this]() {
                return q.size() < static_cast < size_t > (capacity) || shutdownflag;
            });
            if (shutdownflag)
                return false;
            q.push(item);
            lock.unlock();
            notempty.notify_one();
            return true;
        }

        bool pop(T & item) {
            std::unique_lock < std::mutex > lock(mtx);
            notempty.wait(lock, [this]() {
                return !q.empty() || shutdownflag;
            });
            if (q.empty() || shutdownflag)
                return false;
            item = q.front();
            q.pop();
            lock.unlock();
            notfull.notify_one();
            return true;
        }

        void shutdown() {
            {
                std::lock_guard < std::mutex > lock(mtx);
                shutdownflag = true;
            }
            notempty.notify_all();
            notfull.notify_all();
        }
    };

class Validator {
    public: bool validate(const SensorData & data) {
        return data.temperature >= -50 && data.temperature <= 150;
    }
};

class Aggregator {
    private: std::mutex mtx;

    struct Stats {
        double sum = 0;
        int count = 0;
    };

    std::unordered_map < int,
    Stats > sensorStats;

    public: void aggregate(const SensorData & data) {
        std::lock_guard < std::mutex > lock(mtx);
        sensorStats[data.sensorId].sum += data.temperature;
        sensorStats[data.sensorId].count++;
    }

    double getAverage(int sensorId) {
        std::lock_guard < std::mutex > lock(mtx);
        auto it = sensorStats.find(sensorId);
        if (it == sensorStats.end() || it -> second.count == 0)
            return 0;

        return it -> second.sum / it -> second.count;
    }

    void printAverage() {
        std::lock_guard < std::mutex > lock(mtx);
        std::cout << "Average Temperature Data from IoT Device:\n";
        for (auto & stat: sensorStats) {
            double avg = stat.second.count > 0 ? stat.second.sum / stat.second.count : 0.0;
            std::cout << "Average of SensorId " << stat.first <<" Temperature : "<< avg << std::endl;
        }
    }
};

class AlertService {
    public: void checkAlert(const SensorData & data) {
        if (data.temperature > 100) {
            std::cout << "[HIGH TEMP ALERT] Sensor=" << data.sensorId <<
                " Temp=" << data.temperature << std::endl;
        }
    }
};

class Repository {
    public: bool save(const SensorData & data) {
        if (std::rand() % 10 < 3) {
            std::cout << "Failed: sensor data to save for eventid " << data.eventId << std::endl;
            return false;
        }
        std::cout << "Saved the sensor data into database for eventid " << data.eventId << std::endl;
        return true;
    }
};

class DuplicateChecker {
    std::mutex mtx;
    std::unordered_set < long > processedEvents;

    public:
        bool isDuplicateCheck(long eventId) {
            std::lock_guard < std::mutex > lock(mtx);
            auto result = processedEvents.insert(eventId);
            return !result.second; // returns true if duplicate exists
        }
};

class DeadLetterQueue {
    std::mutex mtx;
    std::vector < SensorData > dlq;

    public:
        void push(const SensorData & data) {
            std::lock_guard < std::mutex > lock(mtx);
            dlq.push_back(data);
        }

    size_t size() {
        std::lock_guard < std::mutex > lock(mtx);
        return dlq.size();
    }
};

class SensorProcessor {
    Aggregator & aggregator;
    DuplicateChecker & duplicateChecker;
    Repository repository;
    Validator validator;
    AlertService alertService;
    DeadLetterQueue & dlq;

    public:
        SensorProcessor(Aggregator & agg, DuplicateChecker & dup, DeadLetterQueue & deadLetterQ): aggregator(agg), duplicateChecker(dup), dlq(deadLetterQ) {}

    void process(const SensorData & data) {
        if (duplicateChecker.isDuplicateCheck(data.eventId)) {
            std::cout << "Duplicate Record for EventID " << data.eventId << std::endl;
            return;
        }
        if (!validator.validate(data)) {
            std::cout << "Invalid Temperature for eventId: " << data.eventId << std::endl;
            return;
        }
        aggregator.aggregate(data);
        alertService.checkAlert(data);

        constexpr int MAX = 3;
        for (int i = 1; i <= MAX; i++) {
            if (repository.save(data))
                return;
            std::cout << "Retry " << i << " for eventid " << data.eventId << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * i));
        }
        std::cout << "Max retries failed " << MAX << ". Sensor Data is saved in Dead letter queue for event id " << data.eventId << std::endl;
        dlq.push(data);
    }
};

class IoTGateway {
    ThreadSafeQueue < SensorData > queue;
    Aggregator aggregator;
    DuplicateChecker duplicateChecker;
    DeadLetterQueue dlq;
    std::vector < std::thread > workers;

    public:
        IoTGateway(int workerCount = 4): queue(10) {
            for (int i = 1; i <= workerCount; i++) {
                workers.emplace_back([this, i]() {
                    SensorProcessor processor(aggregator, duplicateChecker, dlq);
                    SensorData data;
                    while (queue.pop(data)) {
                        processor.process(data);
                        std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    }
                    std::cout << "Worker " << i << " Exited\n";
                });
            }
        }

    void receiveData(const SensorData & data) {
        queue.push(data);
    }

    void stop() {
        queue.shutdown();
        for (auto & t: workers) {
            if (t.joinable())
                t.join();
        }
    }

    void printStat() {
        aggregator.printAverage();
    }
};

int main() {
    // Initialize the gateway with 3 worker threads
    IoTGateway gateway(3);

    // Simulate incoming sensor data streams
    for (int i = 1; i <= 10; ++i) {
        SensorData data;
        data.sensorId = 100 + (i % 3);
        data.eventId = 1000 + i;
        data.temperature = 20.0 + (i * 12.0); // Generates various normal and high alert temperatures
        data.timestamp = 1710000000 + i;

        gateway.receiveData(data);
    }

    // Send a duplicate event to test DuplicateChecker
    SensorData duplicateData {
        101,
        1001,
        25.0,
        1710000100
    };
    gateway.receiveData(duplicateData);

    // Send an invalid temperature record to test Validator
    SensorData invalidData {
        102,
        2000,
        250.0,
        1710000200
    };
    gateway.receiveData(invalidData);

    // Give worker threads time to process the queue
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Stop gateway and print final aggregated stats
    gateway.stop();
    gateway.printStat();

    return 0;
}
