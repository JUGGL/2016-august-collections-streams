package us.juggl.twentysixteen.august;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Integer.*;

/**
 * Created by dphillips on 8/6/16.
 */
public class SimpleList {
    public static void main(String[] args) throws Exception {
//        System.out.println("\n\nStandard Stream filter example");
//        simpleFilterListExample();
//        System.out.println("\n\nParallel Streams Example");
//        parallelFilterListExample();
        System.out.println("\n\nParallel word count example using Old Testement King James bible");
        textWordCount("kjvdat.txt");
    }

    /**
     * Using a simple stream pipeline to filter a list
     */
    private static void simpleFilterListExample() {
        // Start with a list of names
        List<String> names = Arrays.asList("John", "Jane", "Adam", "Alexis", "Daniel", "Donna", "Eric", "Evelyn");

        List<String> filteredList = names
                                        .stream()                       // Convert the list to a stream
                                        .filter(n -> n.startsWith("A")) // Use a predicate to filter the list
                                        .collect(Collectors.toList());  // Collect the results back into a new list

        filteredList
                .forEach(n -> System.out.println("\t"+n));
    }

    /**
     * Use parallel streams to genereate UUID strings, sort them, and the compare parallel and non-parallel sort times
     */
    private static void parallelFilterListExample() throws Exception {
        // Generate a VERY long list of Strings
        System.out.println("\tUsing UUID to generate 10,000,000 strings.");
        List<String> randomStrings = IntStream
                                        .range(0, 10_000_000)
                                        .parallel()
                                        .mapToObj(i -> UUID.randomUUID().toString())
                                        .collect(Collectors.toList());

        System.out.println("\tPress ENTER to begin standard sort");
        System.in.read();
        Instant start = Instant.now();
        randomStrings
                .stream()
                .sorted()
                .collect(Collectors.toList());
        Instant end = Instant.now();
        long stdRunTime = end.toEpochMilli() - start.toEpochMilli();
        System.out.println(String.format("\tFinished standard sort in %d milliseconds", stdRunTime));

        System.out.println("\tPress ENTER to begin parallel sort");
        System.in.read();
        start = Instant.now();
        randomStrings
                .parallelStream()
                .sorted()
                .collect(Collectors.toList());
        end = Instant.now();
        long parallelRunTime = end.toEpochMilli() - start.toEpochMilli();
        System.out.println(String.format("\tFinished parallel sort in %d milliseconds", parallelRunTime));
        double pctGain = 100-(((parallelRunTime*1.0)/stdRunTime)*100);
        System.out.println(String.format("\tParallel sort finished %3.2f percent faster than the standard sort", pctGain));
    }

    /**
     * Return the top 5 most frequently used words from the sample text.
     * @throws Exception
     */
    private static void textWordCount(String fileName) throws Exception {
        long start = Instant.now().toEpochMilli();
        ConcurrentHashMap<String, LongAdder> wordCounts = new ConcurrentHashMap<>();
        System.out.println("\tReading file: "+fileName);
        Path filePath = Paths.get(fileName);
        Files.readAllLines(filePath)
            .parallelStream()                               // Start streaming the lines
            .map(line -> line.split("\\s+"))                // Split line into individual words
            .flatMap(Arrays::stream)                        // Convert stream of String[] to stream of String
            .parallel()                                     // Convert to parallel stream
            .filter(w -> w.matches("\\w{6,}"))              // Filter out non-word items
            .map(String::toLowerCase)                       // Convert to lower case
            .forEach(word -> {                              // Use an AtomicAdder to tally word counts
                if (!wordCounts.containsKey(word))          // If a hashmap entry for the word doesn't exist yet
                    wordCounts.put(word, new LongAdder());  // Create a new LongAdder
                wordCounts.get(word).increment();           // Increment the LongAdder for each instance of a word
            });
        wordCounts
                .keySet()
                .stream()
                .map(key -> String.format("%-10d %s", wordCounts.get(key).intValue(), key))
                .sorted((prev, next) -> compare(parseInt(next.split("\\s+")[0]), parseInt(prev.split("\\s+")[0])))
                .limit(5)
                .forEach(t -> System.out.println("\t"+t));
        long end = Instant.now().toEpochMilli();
        System.out.println(String.format("\tCompleted in %d milliseconds", (end-start)));
    }
}
