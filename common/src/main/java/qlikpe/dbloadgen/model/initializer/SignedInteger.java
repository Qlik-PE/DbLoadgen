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
 * Generate a random signed integer in the specified range.
 * Since the nextInt() function only works with min <=0, we
 * have to do a little extra math to get numbers < zero in the mix.
 * Note that the range specified must be within the valid range of
 * the target column or you will risk getting a database exception.
 */
public class SignedInteger implements Initializer {
    private static final Logger LOG = LogManager.getLogger(SignedInteger.class);
    private int min;
    private int max;
    private int bound;

    /**
     * Generate any signed integer within the range specified.
     */
    public SignedInteger() {
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
    public SignedInteger(int min, int max) {
        if (min > max)
            throw new RuntimeException(String.format("invalid range specified: min(%d) max(%d)", min, max));

        configure(min,max);
    }

    public void configure(int min, int max) {
        this.min = min;
        this.max = max;
        bound = (max-min)+1;
        if (bound <= 0)
            throw new RuntimeException(String.format("bound was NOT positive: min(%d) max(%d) bound(%d)",
                    min, max, bound));
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
        else {
            // this works when the value of min is < zero
            value = ThreadLocalRandom.current().nextInt(bound) + min;
            if (value < min)
                throw new RuntimeException(String.format("value was less than min: min(%d) max(%d) bound(%d) value(%d)",
                        min, max, bound, value));
            if (value > max)
                throw new RuntimeException(String.format("value was greater than max: min(%d) max(%d) bound(%d) value(%d)",
                        min, max, bound, value));

     //       LOG.warn("SignedInteger: max: min({}) max({}) bound({}) value({}) string({})",
     //               min, max, bound, value, Integer.toString(value));

        }
        return Integer.toString(value);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("SignedInteger: return a random integer%n"));
        buffer.append(String.format("\tUsage: SignedInteger - any random integer %n"));
        buffer.append(String.format("\t       SignedInteger,min,max - a random integer in the specified range %n"));
        buffer.append(String.format("\t            where min is inclusive, max is exclusive%n"));

        return buffer;
    }
}
