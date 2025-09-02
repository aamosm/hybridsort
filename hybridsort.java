
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Sorts an array by breaking it into chunks, sorting them in parallel,
 * and then merging the sorted chunks.
 */
public class ParallelHybridSort {

    // The size of array segments to be sorted by individual threads.
    private static final int CHUNK_SIZE = 4096;

    // Below this size, Quicksort is inefficient. We switch to Insertion Sort instead.
    private static final int QUICKSORT_THRESHOLD = 47;

    /**
     * The main method to sort the array.
     */
    public static void sort(long[] array) {
        if (array == null || array.length <= 1) {
            return;
        }

        // A single helper array is used for all merge operations to reduce memory allocation.
        long[] helper = new long[array.length];

        // 1. Break the work into chunks and sort them in parallel.
        int coreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(coreCount);
        List<Future<?>> tasks = new ArrayList<>();

        for (int i = 0; i < array.length; i += CHUNK_SIZE) {
            final int start = i;
            // The last chunk might be smaller; this handles that case.
            final int end = Math.min(start + CHUNK_SIZE, array.length);
            final int chunkIndex = i / CHUNK_SIZE;

            // Submit each chunk to the thread pool for sorting.
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

        // 2. Merge the sorted chunks back together.
        mergeChunks(array, helper);
    }

    /**
     * Chooses a sorting algorithm for a given chunk.
     */
    private static void sortChunk(long[] array, int start, int end, int chunkIndex) {
        // Cycle through different sorters for a hybrid approach.
        switch (chunkIndex % 3) {
            case 0:
                insertionSort(array, start, end);
                break;
            case 1:
                maxSelectionSort(array, start, end);
                break;
            case 2:
                quickSort(array, start, end - 1);
                break;
        }
    }

    // --- Sorting Algorithms ---

    private static void insertionSort(long[] arr, int start, int end) {
        for (int i = start + 1; i < end; i++) {
            long current = arr[i];
            int j = i - 1;
            while (j >= start && arr[j] > current) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = current;
        }
    }

    private static void maxSelectionSort(long[] arr, int start, int end) {
        for (int i = end - 1; i > start; i--) {
            int maxIndex = i;
            for (int j = start; j < i; j++) {
                if (arr[j] > arr[maxIndex]) {
                    maxIndex = j;
                }
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

    /**
     * Part of Quicksort. Arranges numbers around a pivot.
     * Uses a median-of-three strategy to pick a good pivot and avoid worst-case performance.
     */
    private static int partition(long[] arr, int low, int high) {
        int mid = low + (high - low) / 2;
        // Order low, mid, and high elements.
        if (arr[low] > arr[mid]) swap(arr, low, mid);
        if (arr[low] > arr[high]) swap(arr, low, high);
        if (arr[mid] > arr[high]) swap(arr, mid, high);

        // Place the median (now at mid) at the second-to-last position to act as the pivot.
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

    /**
     * Merges the sorted chunks together, progressively combining them into larger sorted segments.
     */
    private static void mergeChunks(long[] array, long[] helper) {
        for (int size = CHUNK_SIZE; size < array.length; size = 2 * size) {
            for (int left = 0; left < array.length; left += 2 * size) {
                int mid = Math.min(left + size, array.length);
                int right = Math.min(left + 2 * size, array.length);
                if (mid < right) {
                    merge(array, helper, left, mid, right);
                }
            }
        }
    }

    /**
     * Merges two adjacent sorted segments: [left...mid-1] and [mid...right-1].
     */
    private static void merge(long[] array, long[] helper, int left, int mid, int right) {
        // Copy the relevant section into the helper array to work from.
        System.arraycopy(array, left, helper, left, right - left);

        int leftPtr = left;
        int rightPtr = mid;
        int writePtr = left;

        // Compare elements from both segments and write the smaller one back to the main array.
        while (leftPtr < mid && rightPtr < right) {
            if (helper[leftPtr] <= helper[rightPtr]) {
                array[writePtr++] = helper[leftPtr++];
            } else {
                array[writePtr++] = helper[rightPtr++];
            }
        }

        // If any elements are left in the first segment, copy them over.
        System.arraycopy(helper, leftPtr, array, writePtr, mid - leftPtr);
    }

    private static void swap(long[] arr, int i, int j) {
        long temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    // --- Demo ---
    public static void main(String[] args) {
        final int ARRAY_SIZE = 2_000_000;
        long[] arrayToSort = new Random().longs(ARRAY_SIZE).toArray();
        long[] controlArray = Arrays.copyOf(arrayToSort, arrayToSort.length);
        
        System.out.println("Sorting " + ARRAY_SIZE + " numbers...");

        long startTime = System.nanoTime();
        ParallelHybridSort.sort(arrayToSort);
        long ourDuration = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("-> Custom sort took: " + ourDuration + " ms");
        
        startTime = System.nanoTime();
        Arrays.parallelSort(controlArray);
        long controlDuration = (System.nanoTime() - startTime) / 1_000_000;
        System.out.println("-> Java's built-in sort took: " + controlDuration + " ms");
        
        if (Arrays.equals(arrayToSort, controlArray)) {
            System.out.println("\nVerification: Success");
        } else {
            System.out.println("\nVerification: FAILED");
        }
    }
}
