package us.juggl.twentysixteen.august;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by dphillips on 8/6/16.
 */
public class SimpleList {
    public static void main(String[] args) throws Exception {
        System.out.println("\n\nStandard Stream filter example");
        simpleFilterListExample();
        System.out.println("\n\nParallel Streams Example");
        parallelFilterListExample();
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
        double pctGain = ((parallelRunTime*1.0)/stdRunTime)*100;
        System.out.println(String.format("\tParallel sort finished %3.2f percent faster than the standard sort", pctGain));
    }
}
