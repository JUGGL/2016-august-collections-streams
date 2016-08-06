# Java 8 Collections And Streams Examples

Java 8 introduced a lot of long-awaited improvements to the Java language:
* Lambdas
* Simplifies Date/Time APIs
* Concurrency APIs
* Optionals
* and . . . Streams

Since our topic today is Streams, we must also discuss the changes to the Java 8 
Collections APIs which were made to support Streams. This example project demonstrates
a few of the features of the new APIs using (I hope!) simple and concise code.

# Examples

## A Simple List [Source](src/main/java/us/juggl/twentysixteen/august/SimpleList.java)
This example starts with a list of objects/primitives and performs operations 
using the new Streams API.
* `simpleFilterListExample()` - Filter a list of strings using streams and lambda expressions
* `parallelFilterListExample()` - When you need to filter/sort a LARGE amount of data and want to take advantage of multiple CPU cores
* `genesisWordCount()` - Given a sample of text from Genesis, tally word usages and return top 5 using parallel streams