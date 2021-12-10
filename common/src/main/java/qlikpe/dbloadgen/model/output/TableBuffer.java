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

    private String formatHtmlRow(String tag, String args, String row) {
        StringBuilder formattedRow = new StringBuilder();
        String[] cols = row.split(",");
        formattedRow.append("<tr>");
        for (String col : cols) {
            formattedRow.append(String.format("<%s %s>%s</%s>", tag, args, col, tag));
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
     * @param height the height of the table body.
     * @return a String containing the lines to be output.
     */
    public String asHtml(String height) {
        StringBuilder builder = new StringBuilder();
        int numCols = buffer.get(0).getLine().split(",").length;
        String colClass;
        switch(numCols) {
            case 1: colClass = "col-12"; break;
            case 2: colClass = "col-6"; break;
            case 3: colClass = "col-4"; break;
            case 4: colClass = "col-3"; break;
            case 5:
            case 6: colClass = "col-2"; break;
            default: colClass = "col-1"; break;
        }

        builder.append("<h3 style=\"color: #006580\">").append(getDescription()).append("</h3>");
        //builder.append("<div>");

        builder.append("<table  class=\"table table-fixed\">");
        builder.append("<thead>");
        builder.append(formatHtmlRow("th", String.format("scope=\"col\" class=\"%s\"", colClass), buffer.get(0).getLine()));
        builder.append("</thead>").append(String.format("<tbody style=\"height: %s\">", height));
        for(int i = 1; i < buffer.size(); i++) {
            builder.append(formatHtmlRow("td",  String.format("class=\"%s\"", colClass), buffer.get(i).getLine()));
        }
        builder.append("</tbody>").append("</table>");
        //builder.append("</div>");
        //builder.append(HTML_BREAK);

        return builder.toString();
    }
}
