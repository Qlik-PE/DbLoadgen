/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qlikpe.dbloadgen.model.output;

/**
 * Manages data that should be output as a table. Most typically,
 * this buffer will not accumulate data, but rather will be reset each
 * time it is loaded. Each line that is added should be
 * comma delimited to allow identification of column boundaries
 * when formatting output.
 */
public class TableBuffer extends OutputBuffer {
    private String[] textFormat;

    public TableBuffer(String name) {
        super(name);
    }

    /**
     * Set the format to use when formatting table rows.
     * @param format a "String.format()" format string.
     */
    public void setTextFormat(String format) { textFormat = format.split(","); }

    private String formatTextRow(String row) {
        StringBuilder formattedRow = new StringBuilder();
        String[] cols = row.split(",");
        for(int i = 0; i < cols.length; i++) {
            formattedRow.append(String.format(textFormat[i], cols[i]));
        }
        return formattedRow.toString();
    }

    private String formatHtmlRow(String tag, String row) {
        StringBuilder formattedRow = new StringBuilder();
        String[] cols = row.split(",");
        formattedRow.append("<tr>");
        for (String col : cols) {
            formattedRow.append(String.format("<%s>%s</%s>", tag, col, tag));
        }
        formattedRow.append("</tr>");
        return formattedRow.toString();
    }

    /**
     * Return the buffer formatted as a table for output to the console.
     * @return a string containing each line to be output.
     */
    public String asText() {
        StringBuilder builder = new StringBuilder();
        String dash = "-";
        String header = formatTextRow(buffer.get(0).getLine());
        String divider = dash.repeat(header.length());
        builder.append(NEWLINE);
        builder.append(getDescription()).append(NEWLINE);
        builder.append(divider).append(NEWLINE);
        builder.append(header).append(NEWLINE);
        builder.append(divider).append(NEWLINE);
        for(int i = 1; i < buffer.size(); i++) {
            builder.append(formatTextRow(buffer.get(i).getLine())).append(NEWLINE);
        }
        builder.append(divider).append(NEWLINE);
        return builder.toString();
    }

    /**
     * Return the buffer formatted as HTML.
     * @return a String containing the lines to be output.
     */
    public String asHtml() {
        StringBuilder builder = new StringBuilder();
        builder.append("<h3 style=\"color: #006580\">").append(getDescription()).append("</h3>");
        builder.append("<div class=\"tableFixHead\">");
        builder.append("<table class=\"table table-bordered\">");
        builder.append("<thead>").append("<tr>");
        builder.append(formatHtmlRow("th", buffer.get(0).getLine()));
        builder.append("</tr>").append("</thead>").append("<tbody>");
        for(int i = 1; i < buffer.size(); i++) {
            builder.append(formatHtmlRow("td", buffer.get(i).getLine()));
        }
        builder.append("</tbody>").append("</table>");
        builder.append("</div>");
        builder.append(HTML_BREAK);

        return builder.toString();
    }
}
