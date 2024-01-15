import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HybridSortt {

    private static final int SEGMENT_SIZE = 1000;
    private static final int THREAD_POOL_SIZE = 4;

    public static void customSort(long[] arr, int start, int end) {
        boolean sorted = false;
        while (!sorted) {
            sorted = true;
            for (int i = start; i < end - 1; i++) {
                if (arr[i] > arr[i + 1]) {
                    swap(arr, i, i + 1);
                    sorted = false;
                }
            }
        }
    }

    public static void quickSort(long[] arr, int low, int high) {
        if (low < high) {
            int[] pi = partition(arr, low, high);
            quickSort(arr, low, pi[0] - 1);
            quickSort(arr, pi[1] + 1, high);
        }
    }

    private static int[] partition(long[] arr, int low, int high) {
        int mid = low + (high - low) / 2;
        if (arr[low] > arr[mid]) {
            swap(arr, low, mid);
        }
        if (arr[low] > arr[high]) {
            swap(arr, low, high);
        }
        if (arr[mid] > arr[high]) {
            swap(arr, mid, high);
        }

        long pivot = arr[mid];
        int i = low - 1, j = low, k = high;
        while (j <= k) {
            if (arr[j] < pivot) {
                swap(arr, ++i, j++);
            } else if (arr[j] > pivot) {
                swap(arr, j, k--);
            } else {
                j++;
            }
        }
        return new int[]{i + 1, k};
    }

    private static void swap(long[] arr, int i, int j) {
        long temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void displaySortedArray(long[] arr, int size) {
        System.out.println("Sorted Array: " + Arrays.toString(Arrays.copyOf(arr, size)));
    }

    public static long[] readArrayFromFile(String filePath) {
        try {
            Scanner scanner = new Scanner(new File(filePath));
            String content = scanner.useDelimiter("\\Z").next();
            scanner.close();

            String[] numberStrings = content.replaceAll("[^0-9,]", "").split(",");
            long[] numbers = new long[numberStrings.length];
            for (int i = 0; i < numberStrings.length; i++) {
                numbers[i] = Long.parseLong(numberStrings[i]);
            }

            return numbers;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void measureExecutionTimeAndMemory(Runnable runnable) {
        long startTime = System.nanoTime();
        runnable.run();
        long endTime = System.nanoTime();

        System.out.println("Execution Time: " + (endTime - startTime) + " nanoseconds");
    }

    public static void main(String[] args) {
        System.out.println();
        String filePath = "listofnumbers.txt";
        long[] arrayToSort = readArrayFromFile(filePath);

        if (arrayToSort != null) {
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            executorService.execute(() -> {
                measureExecutionTimeAndMemory(() -> quickSort(arrayToSort, 0, arrayToSort.length - 1));
                displaySortedArray(arrayToSort, SEGMENT_SIZE);
            });

            executorService.execute(() -> {
                measureExecutionTimeAndMemory(() -> customSort(arrayToSort, 0, arrayToSort.length - 1));
                displaySortedArray(arrayToSort, arrayToSort.length);
            });

            executorService.shutdown();
        } else {
            System.out.println("Error reading the input file.");
        }
        System.out.println();
    }
}
