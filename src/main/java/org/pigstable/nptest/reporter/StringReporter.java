package org.pigstable.nptest.reporter;

import org.pigstable.nptest.result.DataSetReport;
import org.pigstable.nptest.result.FieldReport;
import org.pigstable.nptest.result.TupleReport;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

/**
 * An object which can format a DataSet Report in a human-readable manner.
 */
public class StringReporter {

    /**
     * Format the given DataSetReport in a human friendly way.
     *
     * @param report the report to format
     * @return A Human-readable representation of the report
     */
    public static String format(DataSetReport report) throws IOException {
        try (StringWriter writer = new StringWriter()) {
            format(report, writer);
            return writer.toString();
        }
    }

    /**
     * Format the given DataSetReport in a human friendly way.
     * <p/>
     * the given will not be closed.
     *
     * @param report the report to format
     * @param writer the writer to which to write the human-readable report
     */
    public static void format(DataSetReport report, Writer writer) throws IOException {
        writer.append("Validation Report For '").append(report.getName()).append("'\n");

        // -- print the error message if there is one
        if (report.getMessage() != null) {
            writer.append("\t").append(report.getMessage()).append("\n");
        }

        // -- iterate the tuple reports
        writer.append("Tuples: \n");
        String childPrefix = "\t";
        for (TupleReport tupleReport : report.getTupleReports()) {
            format(tupleReport, writer, childPrefix);
        }
    }

    /**
     * Format the given DataSetReport in a human friendly way.
     * <p/>
     * the given will not be closed.
     *
     * @param report the report to format
     * @param writer the writer to which to write the human-readable report
     */
    public static void format(TupleReport report, Writer writer, String linePrefix) throws IOException {
        writer.append(linePrefix).append("- Tuple ").append(report.getKey());

        if (report.getMessage() == null && !report.hasFieldError()) {
            writer.append(": Valid").append("\n");
        } else {
            writer.append("\n");

            // -- print the error message if there is one
            if (report.getMessage() != null) {
                writer.append(linePrefix).append("  ").append(report.getMessage()).append("\n");
            }

            // -- iterate the tuple reports
            String childPrefix = linePrefix + "\t";
            for (FieldReport fieldReport : report.getFieldReports()) {
                format(fieldReport, writer, childPrefix);
            }
        }
    }

    /**
     * Format the given DataSetReport in a human friendly way.
     * <p/>
     * the given will not be closed.
     *
     * @param report the report to format
     * @param writer the writer to which to write the human-readable report
     */
    public static void format(FieldReport report, Writer writer, String linePrefix) throws IOException {
        writer.append(linePrefix).append("- Field #").append(Integer.toString(report.getFieldSequence())).append(": ");

        // -- print the error message if there is one
        if (report.getMessage() != null) {
            writer.append(report.getMessage()).append("\n");
        } else {
            writer.append("Valid\n");
        }
    }
}