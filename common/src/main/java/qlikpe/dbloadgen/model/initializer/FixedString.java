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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Integer.max;

/**
 * Generate a not-so-random string of the specified length. This is done
 * by generating a long random string at instance construction time
 * and returning a string beginning at a randomly selected offset in
 * the source string at runtime. Each index will return a different
 * substring, but the same substring will be returned each time
 * a random offset is repeated.
 *
 * This class is not suitable for use in a primary key.
 */
public class FixedString implements Initializer {
    private final static Logger LOG = LogManager.getLogger(FixedString.class);

    private String source;
    private int maxIndex;
    private int length;

    /**
     * Default constructor.
     */
    public FixedString() {
        this.length = 10;
        configure(length);
    }

    /**
     * Constructor
     * @param length the length of the string that should be returned.
     */
    public FixedString(int length) {
        this.length = length;
        configure(length);
    }

    /**
     * Get the value of length.
     * @return the length of this resource.
     */
    public int getLength() { return length; }

    public void configure(int length) {
        int sourceLength = max(1024, length*3);
        RandomString randomString = new RandomString(RandomString.Type.ALPHANUMERIC, sourceLength);
        source = randomString.nextValue();
        this.length = length;
        this.maxIndex = sourceLength - length;
        if (maxIndex < 0)
            LOG.error("maxIndex < 0: maxIndex({}) sourceLength: ({}) length: ({})",
                    maxIndex, sourceLength, length);
    }

    @Override
    public String nextValue() {
        int index;

        try {
            index = ThreadLocalRandom.current().nextInt(0, maxIndex);
        } catch(IllegalArgumentException e) {
            LOG.error("invalid bound specified. maxxIndex: {} length: {}",
                    maxIndex, length);
            throw e;
        }

        return source.substring(index, index+length);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("FixedString: generate a string of fixed length.%n"));
        buffer.append(String.format("\tRelatively efficient but not terribly random.%n"));
        buffer.append(String.format("\tNot suitable for use in a key column.%n"));
        buffer.append(String.format("\tUsage: FixedString - generate a string of the default length of 10%n"));
        buffer.append(String.format("\t       FixedString,<length> - generate a string of the specified length%n"));

        return buffer;
    }
}
