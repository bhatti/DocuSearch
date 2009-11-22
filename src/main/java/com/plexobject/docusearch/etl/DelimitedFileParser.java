package com.plexobject.docusearch.etl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.validator.GenericValidator;

import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;

/**
 * @author Shahzad Bhatti
 * 
 */
public abstract class DelimitedFileParser implements Runnable {
    static Logger LOGGER = Logger.getLogger(DelimitedFileParser.class);
    public static final char DEFAULT_DELIMITER = ',';
    public static final int MAX_LINES_PER_ROW = 32;
    private final File inputFile;
    private char delimiter;

    private final String[] selectedColumns;
    protected final Map<String, Integer> columnPositions = new HashMap<String, Integer>();

    public DelimitedFileParser(final File inputFile, final char delimiter,
            final String... selectedColumns) {
        if (null == inputFile) {
            throw new NullPointerException("null file");
        }
        if (selectedColumns == null) {
            throw new NullPointerException("null columns");
        }
        if (selectedColumns.length == 0) {
            throw new IllegalArgumentException("no columns");
        }

        for (String col : selectedColumns) {
            if (GenericValidator.isBlankOrNull(col)) {
                throw new IllegalArgumentException("null or empty column in "
                        + Arrays.asList(selectedColumns));
            }
        }
        this.delimiter = delimiter;
        this.inputFile = inputFile;
        this.selectedColumns = selectedColumns;
    }

    public void run() {
        final Timer timer = Metric.newTimer("DelimitedFileParser.run");
        try {
            BufferedReader in = new BufferedReader(new FileReader(inputFile));

            String line = in.readLine();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("header line " + line);
            }
            if (line == null) {
                throw new IllegalArgumentException(inputFile + " is empty");
            }
            final String[] headerColumns = processHeaderRow(StringUtils
                    .splitPreserveAllTokens(line, delimiter));
            int numRow = 1;
            while ((line = in.readLine()) != null) {
                final String[] columns = getColumns(in, line, headerColumns,
                        numRow);

                final Map<String, String> row = makeRow(line, headerColumns,
                        numRow, columns);
                if (!handleRow(numRow, row)) {
                    break;
                }
                numRow++;
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            timer.stop();
        }
    }

    Map<String, String> makeRow(String line, final String[] headerColumns,
            int numRow, final String[] columns) {
        final Map<String, String> row = new HashMap<String, String>();
        for (String selectedCol : selectedColumns) {
            Integer pos = columnPositions.get(selectedCol);
            if (pos == null) {
                throw new RuntimeException("failed to find position of "
                        + selectedCol + " in " + columnPositions);
            }
            if (columns.length <= pos) {
                throw new RuntimeException(numRow + "th " + line
                        + " only contains " + columns.length
                        + ", but selected column " + selectedCol + " requires "
                        + pos);
            }

            row.put(headerColumns[pos], columns[pos].trim());
        }
        return row;
    }

    String[] getColumns(BufferedReader in, String line,
            final String[] headerColumns, int numRow) throws IOException {
        StringBuilder lineBuffer = new StringBuilder(line.trim());
        int numLine = 0;
        int numColumns = countColumns(lineBuffer);
        while (numColumns < headerColumns.length) {
            final String nextLine = in.readLine();
            if (nextLine == null) {
                throw new RuntimeException(
                        "Unexpected end of line while reading row " + line);
            }
            lineBuffer.append(nextLine.trim());
            numColumns = countColumns(lineBuffer);
            numLine++;
        }
        line = lineBuffer.toString();
        if (numColumns != headerColumns.length && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Expected " + headerColumns.length + ", but found "
                    + numColumns + " for row # " + numRow + ": " + line);

        }
        return StringUtils.splitPreserveAllTokens(line, delimiter);
    }

    /**
     * This method is defined by subclass to handle the row
     * 
     * @param rowNum
     *            - row num
     * @param row
     *            - map of name/value
     * @return true - to continue processing the file, false to halt
     */
    protected abstract boolean handleRow(int rowNum, Map<String, String> row);

    protected String[] processHeaderRow(final String[] headerColumns) {
        if (headerColumns == null || headerColumns.length == 0) {
            throw new IllegalArgumentException("header columns not specified");
        }
        int i = 0;
        for (String col : headerColumns) {
            columnPositions.put(col, i++);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("headers " + columnPositions);
        }
        return headerColumns;
    }

    private int countColumns(CharSequence line) {
        int columns = 1;
        for (int i = 0; i < line.length(); i++) {
            if (line.charAt(i) == delimiter) {
                columns++;
            }
        }
        return columns;
    }
}
