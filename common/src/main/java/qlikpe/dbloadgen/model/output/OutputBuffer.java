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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class OutputBuffer {
    private final String name;
    private String description;
    protected final List<OutputLine> buffer;
    protected static final String NEWLINE = System.lineSeparator();
    protected static final String HTML_BREAK = "<br/>";
    public enum Priority {
        INFO("Info"),
        SUCCESS("Success"),
        WARNING("Warning"),
        ERROR("Error");

        public final String label;

        Priority(String label) { this.label = label; }
        @Override
        public String toString() { return this.label; }
    }

    static class OutputLine {
        private final String line;
        private final Priority priority;

        public OutputLine(String line) {
            this.line = line;
            this.priority = Priority.INFO;
        }

        public OutputLine(Priority status, String line) {
            this.line = line;
            this.priority = status;
        }

        public String getLine() { return line; }
        public Priority getPriority() { return priority; }
    }

    public OutputBuffer(String name) {
        this.name = name;
        this.description = "not set";
        buffer = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Get the description of this buffer.
     * @return the description as a String.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Get the ID (name) of this buffer.
     * @return return the ID as a String.
     */
    public String getName() { return name; }

    /**
     * Set a test description for this buffer.
     * @param description a description as a String.
     */
    public void setDescription(String description) { this.description = description; }

    /**
     * Adds the given line to the output buffer.
     * @param line a line to append to the buffer.
     */
    public void addLine(String line) {
        buffer.add(new OutputLine(line));
    }

    /**
     * Add a line to the buffer with the given status.
     * @param priority the priority of the line/message.
     * @param line the text to display.
     */
    public void addLine(Priority priority, String line) { buffer.add(new OutputLine(priority, line)); }

    /**
     * Reset the buffer so that we can reload it.
     */
    public void resetBuffer() {
        buffer.clear();
    }

    /**
     * Return the buffer formatted for output to the console.
     * @return a the data to be output.
     */
    public abstract String asText();

    /**
     * Return the buffer formatted as HTML for output to a browser.
     * @param height the height of the containing div.
     * @return the data to be output.
     */
    public abstract String asHtml(String height);
}
