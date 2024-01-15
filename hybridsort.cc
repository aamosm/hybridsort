#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>

const int SEGMENT_SIZE = 1000;

void customSort(std::vector<int>& arr, int start, int end) {
    bool sorted = false;
    while (!sorted) {
        sorted = true;
        for (int i = start; i < end - 1; i++) {
            if (arr[i] > arr[i + 1]) {
                std::swap(arr[i], arr[i + 1]);
                sorted = false;
            }
        }
    }
}

void quickSort(std::vector<int>& arr, int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        quickSort(arr, low, pi - 1);
        quickSort(arr, pi + 1, high);
    }
}

int partition(std::vector<int>& arr, int low, int high) {
    int pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
            i++;
            std::swap(arr[i], arr[j]);
        }
    }
    std::swap(arr[i + 1], arr[high]);
    return (i + 1);
}

void displaySortedArray(const std::vector<int>& arr, int size) {
    std::cout << "Sorted Array: ";
    for (int i = 0; i < size; i++) {
        std::cout << arr[i] << " ";
    }
    std::cout << std::endl;
}

std::vector<int> readArrayFromFile(const std::string& filePath) {
    std::vector<int> numbers;
    std::ifstream file(filePath);
    std::string str;
    while (std::getline(file, str, ',')) {
        numbers.push_back(std::stoi(str));
    }
    return numbers;
}

void measureExecutionTimeAndMemory(std::function<void()> func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Execution Time: " << std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count() << " nanoseconds" << std::endl;
}

int main() {
    std::cout << std::endl;
    std::string filePath = "listofnumbers.txt";
    std::vector<int> arrayToSort = readArrayFromFile(filePath);

    if (!arrayToSort.empty()) {
        std::thread t1(& {
            measureExecutionTimeAndMemory(& { quickSort(arrayToSort, 0, arrayToSort.size() - 1); });
            displaySortedArray(arrayToSort, SEGMENT_SIZE);
        });
        t1.join();

        std::thread t2(& {
            measureExecutionTimeAndMemory(& { customSort(arrayToSort, 0, arrayToSort.size() - 1); });
            displaySortedArray(arrayToSort, arrayToSort.size());
        });
        t2.join();
    } else {
        std::cout << "Error reading the input file." << std::endl;
    }
    std::cout << std::endl;

    return 0;
}
