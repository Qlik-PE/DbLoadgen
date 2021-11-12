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
 * Manages a buffer that contains lines of text that should be formatted and output
 * when requested.
 */
public class TextBuffer extends OutputBuffer {
    public TextBuffer(String name) {
        super(name);
    }

    /**
     * Return the buffer as a String for output to the console.
     * @return a string containing each line to be output.
     */
    public String asText() {
        StringBuilder builder = new StringBuilder();
        builder.append(getDescription()).append(NEWLINE);
        for(OutputLine line : buffer) {
            String output = String.format("%s: %s", line.getPriority(), line.getLine());
            builder.append(output).append(NEWLINE);
        }
        return builder.toString();
    }

    /**
     * Return the buffer formatted as HTML.
     * @return a String containing the lines to be output.
     */
    public String asHtml() {
        String color;
        StringBuilder builder = new StringBuilder();
        builder.append("<h3 style=\"color: #006580\">").append(getDescription()).append("</h3>");
        for(OutputLine line : buffer) {
            switch(line.getPriority()) {
                case INFO: color = "steelblue"; break;
                case SUCCESS: color = "green"; break;
                case WARNING: color = "darkorange"; break;
                case ERROR: color = "darkred"; break;
                default: color = "indigo"; break;
            }
            String priority = String.format("<span style=\"color: %s\"><b>%s:</b></span>&emsp;", color, line.getPriority());
            builder.append(priority).append(line.getLine()).append(HTML_BREAK);
        }
        builder.append(HTML_BREAK).append(HTML_BREAK);
        return builder.toString();
    }

}
