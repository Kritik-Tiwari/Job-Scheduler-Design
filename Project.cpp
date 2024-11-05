#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include <fstream>
#include <iomanip>  // For formatting output

using namespace std;

struct Job {
    int id;
    int arrivalDay;
    int arrivalHour;
    int coresRequired;
    int memoryRequired;
    int executionTime;
    int remainingTime;
};

struct WorkerNode {
    int id;
    int availableCores;
    int availableMemory;

    // Constructor to initialize id, cores, and memory
    WorkerNode(int id) : id(id), availableCores(24), availableMemory(64) {}
};

class JobScheduler {
    int numWorkerNodes = 128;
    vector<WorkerNode> workerNodes;
    queue<Job> jobQueue;

public:
    JobScheduler() {
        // Initialize worker nodes
        for (int i = 0; i < numWorkerNodes; ++i) {
            workerNodes.push_back(WorkerNode(i));
        }
    }

    // Method to add jobs to the queue
    void addJob(const Job &job) {
        jobQueue.push(job);
        printJobDetails(job);  // Print job details in the specified format
    }

    // Job Scheduling Policies
    void scheduleJobs(int policy);
    bool allocateJobToWorker(Job &job, int fitPolicy);

    // Print job details in specified format
    void printJobDetails(const Job &job);

    // Statistics and Output
    void logStatistics(int day);
};

// Comparator for Smallest Job First
bool compareSmallestJobFirst(const Job &a, const Job &b) {
    return (a.executionTime * a.coresRequired * a.memoryRequired) < 
           (b.executionTime * b.coresRequired * b.memoryRequired);
}

// Comparator for Shortest Duration First
bool compareShortestDurationFirst(const Job &a, const Job &b) {
    return a.executionTime < b.executionTime;
}

void JobScheduler::scheduleJobs(int policy) {
    // Sort jobs according to policy
    vector<Job> sortedJobs;
    while (!jobQueue.empty()) {
        sortedJobs.push_back(jobQueue.front());
        jobQueue.pop();
    }

    if (policy == 1) { // FCFS, no sorting needed
        // Already in order
    } else if (policy == 2) { // Smallest Job First
        sort(sortedJobs.begin(), sortedJobs.end(), compareSmallestJobFirst);
    } else if (policy == 3) { // Shortest Duration First
        sort(sortedJobs.begin(), sortedJobs.end(), compareShortestDurationFirst);
    }

    // Try to allocate each job
    for (auto &job : sortedJobs) {
        if (!allocateJobToWorker(job, 1)) { // 1 = First Fit (modify as needed)
            jobQueue.push(job); // Reinsert into queue if not allocated
        }
    }
}

bool JobScheduler::allocateJobToWorker(Job &job, int fitPolicy) {
    int selectedNode = -1;

    // First Fit
    if (fitPolicy == 1) {
        for (auto &node : workerNodes) {
            if (node.availableCores >= job.coresRequired &&
                node.availableMemory >= job.memoryRequired) {
                selectedNode = node.id;
                break;
            }
        }
    }

    // (Implement Best Fit and Worst Fit similarly)

    // If a node is selected, allocate resources
    if (selectedNode != -1) {
        workerNodes[selectedNode].availableCores -= job.coresRequired;
        workerNodes[selectedNode].availableMemory -= job.memoryRequired;
        // Schedule job for its duration (decrement remainingTime daily)
        job.remainingTime = job.executionTime;
        return true;
    }
    return false;
}

// Print job details in the specified format
void JobScheduler::printJobDetails(const Job &job) {
    cout << "JobId: " << setw(10) << job.id 
         << " Arrival Day: " << setw(3) << job.arrivalDay 
         << " Time Hour: " << setw(3) << job.arrivalHour 
         << " MemReq: " << setw(3) << job.memoryRequired 
         << " CPUReq: " << setw(3) << job.coresRequired 
         << " ExeTime: " << setw(3) << job.executionTime 
         << endl;
}

void JobScheduler::logStatistics(int day) {
    ofstream file("scheduler_stats_day_" + to_string(day) + ".csv");

    if (file.is_open()) {
        file << "NodeID, CPU_Utilization(%), Memory_Utilization(%)\n";
        for (const auto &node : workerNodes) {
            double cpuUtilization = 100.0 * (24 - node.availableCores) / 24;
            double memoryUtilization = 100.0 * (64 - node.availableMemory) / 64;
            file << node.id << "," << cpuUtilization << "," << memoryUtilization << "\n";
        }
        file.close();
    }
}

int main() {
    JobScheduler scheduler;

    // Simulate multiple days of job arrivals
    for (int day = 0; day < 30; ++day) {
        // Generate jobs for the day
        for (int i = 0; i < 100; ++i) { // Assume 100 jobs per day
            scheduler.addJob({i, day, 0, rand() % 24 + 1, rand() % 64 + 1, rand() % 24 + 1, 0});
        }

        // Schedule jobs based on FCFS and First Fit (modify as needed)
        scheduler.scheduleJobs(1); // 1 = FCFS

        // Log daily statistics
        scheduler.logStatistics(day);
    }

    return 0;
}
