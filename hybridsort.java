import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class HybridSort {

    private static final int SEGMENT_SIZE = 1000;
    private static final int THREAD_POOL_SIZE = 2;

    private static final Logger logger = Logger.getLogger(HybridSort.class.getName());

    static {
        try {
            FileHandler fileHandler = new FileHandler("hybrid_sort.log");
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setLevel(Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void selectionSort(long[] arr, long[] bubbleInsertionArray) {
        long startTime = System.nanoTime();
        for (int i = 0; i < arr.length; i++) {
            int minIndex = i;
            for (int j = i + 1; j < arr.length; j++) {
                if (arr[j] < arr[minIndex]) {
                    minIndex = j;
                }
            }
            swap(arr, i, minIndex);

            if (i == SEGMENT_SIZE - 1) {
                break;
            }
        }
        System.arraycopy(arr, 0, bubbleInsertionArray, 0, arr.length);
        long endTime = System.nanoTime();
        logger.info("Selection Sort Execution Time: " + (endTime - startTime) + " nanoseconds");
    }

    public static void bubbleInsertionSort(long[] arr, int start, int end) {
        long startTime = System.nanoTime();
        for (int i = start; i < end; i++) {
            for (int j = start; j < end - 1; j++) {
                if (arr[j] > arr[j + 1]) {
                    swap(arr, j, j + 1);
                }
            }
        }
        insertionSort(arr, start, end);
        long endTime = System.nanoTime();
        logger.info("Bubble-Insertion Sort Execution Time: " + (endTime - startTime) + " nanoseconds");
    }

    public static void insertionSort(long[] arr, int start, int end) {
        long startTime = System.nanoTime();
        for (int i = start + 1; i < end; i++) {
            long key = arr[i];
            int j = i - 1;
            while (j >= start && arr[j] > key) {
                arr[j + 1] = arr[j];
                j--;
            }
            arr[j + 1] = key;
        }
        long endTime = System.nanoTime();
        logger.info("Insertion Sort Execution Time: " + (endTime - startTime) + " nanoseconds");
    }

    private static void swap(long[] arr, int i, int j) {
        long temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void displaySortedArray(long[] arr, int start, int end) {
        logger.info("Sorted Array: " + Arrays.toString(Arrays.copyOfRange(arr, start, end)));
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

    public static void main(String[] args) {
        logger.info("Starting Hybrid Sort");
        System.out.println();

        String filePath = "listofnumbers.txt";
        long[] arrayToSort = readArrayFromFile(filePath);
        long[] bubbleInsertionArray = Arrays.copyOf(arrayToSort, arrayToSort.length);

        if (arrayToSort != null) {
            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

            Future<?> selectionSortFuture = executorService.submit(() -> {
                            logger.info("Executing Selection Sort");
                            selectionSort(arrayToSort, bubbleInsertionArray);
                            displaySortedArray(arrayToSort, 0, SEGMENT_SIZE);
                    });

            Future<?> bubbleInsertionSortFuture = executorService.submit(() -> {
                            logger.info("Executing Bubble-Insertion Sort");
                            int middle = arrayToSort.length / 2;
                            bubbleInsertionSort(bubbleInsertionArray, 0, middle);
                            displaySortedArray(bubbleInsertionArray, 0, arrayToSort.length);
                    });

            try {

                selectionSortFuture.get();
                bubbleInsertionSortFuture.get();

                compareSortingResults(arrayToSort, bubbleInsertionArray);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                executorService.shutdown();
            }
        } else {
            logger.severe("Error reading the input file.");
        }

        logger.info("Hybrid Sort Completed");
        System.out.println();
    }

    private static void compareSortingResults(long[] arr1, long[] arr2) {

        Arrays.sort(arr1);

        // Log the sorted arrays
        logger.info("Sorted Array (Your Custom Algorithm): " + Arrays.toString(arr2));
        logger.info("Sorted Array (Arrays.sort()): " + Arrays.toString(arr1));

        // Check if both arrays are equal
        if (Arrays.equals(arr1, arr2)) {
            logger.info("Sorting results match Arrays.sort()");
        } else {
            logger.warning("Sorting results do not match Arrays.sort()");
        }

        // WARNING! : java's in built sorting function may not be a very reliable test
    }

}
