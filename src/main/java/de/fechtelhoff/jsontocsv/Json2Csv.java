package de.fechtelhoff.jsontocsv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;

@SuppressWarnings({"java:S6212"})// java:S6212 -> Local-Variable Type Inference should be used
public class Json2Csv {

	private static final Logger LOGGER = LoggerFactory.getLogger(Json2Csv.class);

	public static final String UNHANDLED_EXCEPTION_OCCURRED = "Unhandled exception occurred.";

	public static void main(String[] args) {
		LOGGER.info("");
		LOGGER.info("START");
		LOGGER.info("");
		if (args.length == 1) {
			final Path workDirPath = Path.of(args[0].trim());
			LOGGER.info("Work Directory: \"{}\"", workDirPath);
			LOGGER.info("");
			new Json2Csv().run(workDirPath);
		} else {
			LOGGER.error("Please specify a Work Directory as the one and only Parameter.");
			LOGGER.info("");
		}
		LOGGER.info("");
		LOGGER.info("END");
		LOGGER.info("");
	}

	private void run(final Path workDirPath) {
		for (final Path inputFilePath : getJsonFilesInDirectory(workDirPath)) {
			final File inputFile = inputFilePath.toFile();
			final File outputFile = new File(inputFilePath.toString().replace(".json", ".csv"));
			LOGGER.info("Convert Input File: \"{}\" to Output File \"{}\"", inputFile, outputFile);
			convertJsonToCsv(inputFile, outputFile);
		}
	}

	private void convertJsonToCsv(final File inputFile, final File outputFile) {
		JsonNode jsonTree = null;
		try {
			jsonTree = new ObjectMapper().readTree(inputFile);
		} catch (final IOException exception) {
			LOGGER.error("Could not read Input File: \"{}\"", inputFile);
		}

		Builder csvSchemaBuilder = CsvSchema.builder();
		JsonNode firstObject = Objects.requireNonNull(jsonTree).elements().next();
		firstObject.fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();

		try {
			final CsvMapper csvMapper = new CsvMapper();
			csvMapper.writerFor(JsonNode.class)
				.with(csvSchema)
				.writeValue(outputFile, jsonTree);
		} catch (final IOException exception) {
			LOGGER.error("Could not write Output File: \"{}\"", outputFile);
		}
	}

	@SuppressWarnings("SameParameterValue")
	private List<Path> getJsonFilesInDirectory(final Path directory) {
		try (final Stream<Path> filesWalkStream = Files.walk(directory)) {
			return filesWalkStream
				.parallel()
				.filter(Files::isRegularFile)
				.filter(path -> path.toString().endsWith(".json"))
				.collect(Collectors.toList());
		} catch (final IOException exception) {
			throw new IllegalStateException(UNHANDLED_EXCEPTION_OCCURRED, exception);
		}
	}
}
