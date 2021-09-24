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
package qlikpe.dbloadgen.model.initializer;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate a random integer of a specified length that is padded on the left
 * with zeros if needed.
 */
public class PaddedInteger implements Initializer {
    private static final String zeros = "000000000000000000000000000";
    int length;

    public PaddedInteger() {
        super();
        this.length = 6;
    }
    public PaddedInteger(int length) {
        super();
        this.length = length;
    }

    public void configure(int length) { this.length = length; }

    /**
     * Return a random integer padded on the right with zeros if needed.
     * @return a random value.
     */
    @Override
    public synchronized String nextValue() {
        String value = Integer.toString(ThreadLocalRandom.current().nextInt());

        if (value.length() < length) {
            String zeroPad = zeros.substring(0, length-value.length());
            value = zeroPad.concat(value);
        }

        return value.substring(0, length);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("PaddedInteger: return a random integer%n"));
        buffer.append(String.format("\tpadded on the left with zeros if necessary.%n"));
        buffer.append(String.format("\tUsage: PaddedInteger,<length>%n"));

        return buffer;
    }
}
