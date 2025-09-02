import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Sorts a large array by fetching it from a URL, breaking it into dynamic chunks,
 * sorting them in parallel, and then merging the sorted chunks.
 */
public class ParallelHybridSort {

    // Below this size, Quicksort is inefficient. We switch to Insertion Sort instead.
    private static final int QUICKSORT_THRESHOLD = 47;

    /**
     * The main method to sort the array. It now calculates the chunk size dynamically.
     */
    public static void sort(long[] array) {
        if (array == null || array.length <= 1) {
            return;
        }

        // A single helper array is used for all merge operations to reduce memory allocation.
        long[] helper = new long[array.length];

        // 1. Calculate the best chunk size for this specific array and system.
        int chunkSize = calculateChunkSize(array);
        System.out.println("-> System has " + Runtime.getRuntime().availableProcessors() + " cores. Using dynamic chunk size of " + chunkSize + ".");

        // 2. Break the work into chunks and sort them in parallel.
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<?>> tasks = new ArrayList<>();

        for (int i = 0; i < array.length; i += chunkSize) {
            final int start = i;
            final int end = Math.min(start + chunkSize, array.length);
            final int chunkIndex = i / chunkSize;

            tasks.add(executor.submit(() -> sortChunk(array, start, end, chunkIndex)));
        }

        // Wait for all parallel sorting tasks to complete.
        try {
            for (Future<?> task : tasks) {
                task.get();
            }
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

        // 3. Merge the sorted chunks back together.
        mergeChunks(array, helper, chunkSize);
    }

    /**
     * Dynamically calculates an optimal chunk size.
     * The goal is to create enough chunks to keep all CPU cores busy,
     * but not so small that the overhead of managing them becomes a problem.
     */
    private static int calculateChunkSize(long[] array) {
        int cores = Runtime.getRuntime().availableProcessors();
        int n = array.length;

        // Aim for roughly 4 chunks per core to ensure cores are never idle.
        int desiredChunks = cores * 4;

        // Calculate chunk size, but enforce a reasonable minimum and maximum.
        int chunkSize = n / desiredChunks;
        if (chunkSize < 1024) chunkSize = 1024;       // Minimum chunk size
        if (chunkSize > 65536) chunkSize = 65536;     // Maximum chunk size

        // Ensure the last chunk isn't excessively small.
        if (n / chunkSize > 0 && n % chunkSize < chunkSize / 2) {
             chunkSize = (int) Math.ceil((double) n / (n / chunkSize));
        }

        return chunkSize;
    }

    /**
     * Chooses a sorting algorithm for a given chunk.
     */
    private static void sortChunk(long[] array, int start, int end, int chunkIndex) {
        switch (chunkIndex % 3) {
            case 0: insertionSort(array, start, end); break;
            case 1: maxSelectionSort(array, start, end); break;
            case 2: quickSort(array, start, end - 1); break;
        }
    }

    // --- Sorting Algorithms (Insertion, Max Selection, QuickSort) ---

    private static void insertionSort(long[] arr, int start, int end) {
        for (int i = start + 1; i < end; i++) {
            long current = arr[i];
            int j = i - 1;
            while (j >= start && arr[j] > current) { arr[j + 1] = arr[j--]; }
            arr[j + 1] = current;
        }
    }

    private static void maxSelectionSort(long[] arr, int start, int end) {
        for (int i = end - 1; i > start; i--) {
            int maxIndex = i;
            for (int j = start; j < i; j++) {
                if (arr[j] > arr[maxIndex]) { maxIndex = j; }
            }
            swap(arr, maxIndex, i);
        }
    }

    private static void quickSort(long[] arr, int low, int high) {
        if (high - low < QUICKSORT_THRESHOLD) {
            insertionSort(arr, low, high + 1);
            return;
        }
        if (low < high) {
            int pivotIndex = partition(arr, low, high);
            quickSort(arr, low, pivotIndex - 1);
            quickSort(arr, pivotIndex + 1, high);
        }
    }

    private static int partition(long[] arr, int low, int high) {
        int mid = low + (high - low) / 2;
        if (arr[low] > arr[mid]) swap(arr, low, mid);
        if (arr[low] > arr[high]) swap(arr, low, high);
        if (arr[mid] > arr[high]) swap(arr, mid, high);
        swap(arr, mid, high - 1);
        long pivot = arr[high - 1];
        int i = low, j = high - 1;
        while (true) {
            while (arr[++i] < pivot);
            while (arr[--j] > pivot);
            if (i >= j) break;
            swap(arr, i, j);
        }
        swap(arr, i, high - 1);
        return i;
    }

    // --- Merging Logic ---

    private static void mergeChunks(long[] array, long[] helper, int chunkSize) {
        for (int size = chunkSize; size < array.length; size = 2 * size) {
            for (int left = 0; left < array.length; left += 2 * size) {
                int mid = Math.min(left + size, array.length);
                int right = Math.min(left + 2 * size, array.length);
                if (mid < right) {
                    merge(array, helper, left, mid, right);
                }
            }
        }
    }

    private static void merge(long[] array, long[] helper, int left, int mid, int right) {
        System.arraycopy(array, left, helper, left, right - left);
        int leftPtr = left, rightPtr = mid, writePtr = left;
        while (leftPtr < mid && rightPtr < right) {
            if (helper[leftPtr] <= helper[rightPtr]) {
                array[writePtr++] = helper[leftPtr++];
            } else {
                array[writePtr++] = helper[rightPtr++];
            }
        }
        System.arraycopy(helper, leftPtr, array, writePtr, mid - leftPtr);
    }

    private static void swap(long[] arr, int i, int j) {
        long temp = arr[i]; arr[i] = arr[j]; arr[j] = temp;
    }

    // --- Verification and Display Helpers ---

    /**
     * Quickly checks if the array is properly sorted.
     */
    private static boolean isSorted(long[] array) {
        for (int i = 0; i < array.length - 1; i++) {
            if (array[i] > array[i + 1]) {
                System.out.println("Error: Found unsorted element at index " + i + "!");
                return false;
            }
        }
        return true;
    }

    /**
     * Displays the first and last 10 elements of the array to provide a sample.
     */
    private static void displaySample(long[] array) {
        System.out.println("\n--- Sorted Array Sample ---");
        int n = array.length;
        if (n <= 20) {
            System.out.println(Arrays.toString(array));
        } else {
            System.out.print("First 10: [");
            for (int i = 0; i < 10; i++) System.out.print(array[i] + (i == 9 ? "" : ", "));
            System.out.println("]");

            System.out.print("Last 10:  [");
            for (int i = n - 10; i < n; i++) System.out.print(array[i] + (i == n - 1 ? "" : ", "));
            System.out.println("]");
        }
        System.out.println("-------------------------");
    }

    // --- Main Application Entry Point ---
    public static void main(String[] args) {
        String urlString = "https://raw.githubusercontent.com/aamosm/hybridsort/refs/heads/main/listofnumbers.txt";
        System.out.println("Attempting to download and sort numbers from:");
        System.out.println(urlString);

        try {
            // 1. Download and parse the numbers
            URL url = new URL(urlString);
            BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
            String line = reader.lines().collect(Collectors.joining());
            reader.close();
            
            long[] numbers = Arrays.stream(line.trim().split("\\s*,\\s*"))
                                   .mapToLong(Long::parseLong)
                                   .toArray();

            System.out.println("\nSuccessfully loaded " + numbers.length + " numbers.");
            System.out.println("Starting sort...");

            // 2. Sort the array
            long startTime = System.nanoTime();
            ParallelHybridSort.sort(numbers);
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            System.out.println("-> Sort completed in: " + duration + " ms");

            // 3. Verify the result
            System.out.println("\nVerifying sort...");
            if (isSorted(numbers)) {
                System.out.println("-> Verification: SUCCESS!");
                displaySample(numbers);
            } else {
                System.out.println("-> Verification: FAILED.");
            }

        } catch (Exception e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
