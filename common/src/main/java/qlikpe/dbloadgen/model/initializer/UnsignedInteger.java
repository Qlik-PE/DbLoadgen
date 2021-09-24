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

/**
 * Generate a random unsigned integer in the specified range.
 * Note that the range specified must be within the valid range of
 * the target column or you will risk getting a database exception.
 */
public class UnsignedInteger implements Initializer {
    private static final Logger LOG = LogManager.getLogger(UnsignedInteger.class);
    private int min;
    private int max;

    /**
     * Generate any integer within the range of the random
     * number generator.
     */
    public UnsignedInteger() {
        configure(-1, -1);
    }

    /**
     * Generate an integer within the specified range. If min and
     * max are set to the same value, then the range is ignored and
     * any value within range of the random number generator will
     * be returned.
     * @param min the minimum acceptable value (inclusive).
     * @param max tha maximum acceptable value (exclusive).
     */
    public UnsignedInteger(int min, int max) {
        if (min > max)
            throw new RuntimeException(String.format("invalid range specified: min(%d) max(%d)", min, max));
        if (min < 0)
            throw new RuntimeException(String.format("invalid range specified: min(%d) must be greater than" +
                                  " or equal to zero", min));

        configure(min,max);
    }

    public void configure(int min, int max) {
        this.min = min;
        this.max = max;
    }

    /**
     * Return a random integer.
     * @return a random value.
     */
    @Override
    public synchronized String nextValue() {
        int value;
        if (min == max)
            value = ThreadLocalRandom.current().nextInt();
        else value = ThreadLocalRandom.current().nextInt(min, max);
 //       LOG.warn("UnsignedInteger");
        return Integer.toUnsignedString(value);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("UnsignedInteger: return a random integer%n"));
        buffer.append(String.format("\tUsage: UnsignedInteger - any random integer >= zero %n"));
        buffer.append(String.format("\t       UnsignedInteger,min,max - a random integer in the specified range %n"));
        buffer.append(String.format("\t            where min is inclusive, max is exclusive and %n"));
        buffer.append(String.format("\t            min must be greater than or equal to zero %n"));

        return buffer;
    }
}
