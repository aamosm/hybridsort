import threading
import time
from typing import List

SEGMENT_SIZE = 1000
THREAD_POOL_SIZE = 4

def custom_sort(arr: List[int], start: int, end: int) -> None:
    sorted = False
    while not sorted:
        sorted = True
        for i in range(start, end - 1):
            if arr[i] > arr[i + 1]:
                arr[i], arr[i + 1] = arr[i + 1], arr[i]
                sorted = False

def quick_sort(arr: List[int], low: int, high: int) -> None:
    if low < high:
        pi = partition(arr, low, high)
        quick_sort(arr, low, pi[0] - 1)
        quick_sort(arr, pi[1] + 1, high)

def partition(arr: List[int], low: int, high: int) -> List[int]:
    mid = low + (high - low) // 2
    if arr[low] > arr[mid]:
        arr[low], arr[mid] = arr[mid], arr[low]
    if arr[low] > arr[high]:
        arr[low], arr[high] = arr[high], arr[low]
    if arr[mid] > arr[high]:
        arr[mid], arr[high] = arr[high], arr[mid]

    pivot = arr[mid]
    i = low - 1
    j = low
    k = high
    while j <= k:
        if arr[j] < pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]
            j += 1
        elif arr[j] > pivot:
            arr[j], arr[k] = arr[k], arr[j]
            k -= 1
        else:
            j += 1
    return [i + 1, k]

def display_sorted_array(arr: List[int], size: int) -> None:
    print("Sorted Array: ", arr[:size])

def read_array_from_file(file_path: str) -> List[int]:
    with open(file_path, 'r') as file:
        content = file.read()
        number_strings = content.replace("\n", "").split(",")
        numbers = [int(num_str) for num_str in number_strings]
        return numbers

def measure_execution_time_and_memory(runnable) -> None:
    start_time = time.time_ns()
    runnable()
    end_time = time.time_ns()
    print("Execution Time: ", end_time - start_time, "nanoseconds")

def main():
    print()
    file_path = "listofnumbers.txt"
    array_to_sort = read_array_from_file(file_path)

    if array_to_sort is not None:
        executor = threading.Thread(target=measure_execution_time_and_memory, args=(lambda: quick_sort(array_to_sort, 0, len(array_to_sort) - 1),))
        executor.start()
        display_sorted_array(array_to_sort, SEGMENT_SIZE)

        executor = threading.Thread(target=measure_execution_time_and_memory, args=(lambda: custom_sort(array_to_sort, 0, len(array_to_sort) - 1),))
        executor.start()
        display_sorted_array(array_to_sort, len(array_to_sort))

    else:
        print("Error reading the input file.")
    print()

if __name__ == "__main__":
    main()
