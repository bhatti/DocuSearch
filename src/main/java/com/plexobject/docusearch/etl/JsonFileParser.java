package com.plexobject.docusearch.etl;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.GenericValidator;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.plexobject.docusearch.metrics.Metric;
import com.plexobject.docusearch.metrics.Timer;

/**
 * @author Shahzad Bhatti
 * 
 */
public abstract class JsonFileParser implements Runnable {
    static Logger LOGGER = Logger.getLogger(JsonFileParser.class);
    private final File inputFile;

    private final String[] selectedColumns;

    public JsonFileParser(final File inputFile, final String... selectedColumns) {
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
        this.inputFile = inputFile;
        this.selectedColumns = selectedColumns;
    }

    public void run() {
        final Timer timer = Metric.newTimer("JsonFileParser.run");
        try {
            final String jsonStr = FileUtils.readFileToString(inputFile);
            JSONArray jsonArr;
            jsonArr = new JSONArray(jsonStr);
            for (int i = 0; i < jsonArr.length(); i++) {
                JSONObject rawJson = jsonArr.getJSONObject(i);
                JSONObject json = new JSONObject();

                for (String col : selectedColumns) {
                    json.put(col, rawJson.get(col));
                }
                if (!handleJson(i, json)) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (JSONException e) {
            throw new RuntimeException(e);

        } finally {
            timer.stop();
        }
    }

    /**
     * This method is defined by subclass to handle the row
     * 
     * @param rowNum
     *            - row num
     * @param json
     *            - json object
     * @return true - to continue processing the file, false to halt
     */
    protected abstract boolean handleJson(int rowNum, JSONObject json);
}
