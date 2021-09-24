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
 * Generate a random signed long in the specified range.
 * Since the nextLong function only works with min <=0, we
 * have to do a little extra math to get numbers < zero in the mix.
 * Note that the range specified must be within the valid range of
 * the target column or you will risk getting a database exception.
 */
public class SignedLong implements Initializer {
    private long min;
    private long max;
    private long bound;

    /**
     * Generate any signed long within the range specified.
     */
    public SignedLong() {
        configure(-1, -1);
    }

    /**
     * Generate an long within the specified range. If min and
     * max are set to the same value, then the range is ignored and
     * any value within range of the random number generator will
     * be returned.
     * @param min the minimum acceptable value (inclusive).
     * @param max tha maximum acceptable value (exclusive).
     */
    public SignedLong(long min, long max) {
        if (min > max)
            throw new RuntimeException(String.format("invalid range specified: min(%d) max(%d)", min, max));

        configure(min,max);
    }

    public void configure(long min, long max) {
        this.min = min;
        this.max = max;
        bound = (max-min)+1;
    }

    /**
     * Return a random long.
     * @return a random value.
     */
    @Override
    public synchronized String nextValue() {
        long value;
        if (min == max)
            value = ThreadLocalRandom.current().nextLong();
        else {
            // this works when the value of min is < zero
            value = ThreadLocalRandom.current().nextLong(bound) + min;
        }
        return Long.toString(value);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("SignedLong: return a random long%n"));
        buffer.append(String.format("\tUsage: SignedLong - any random long %n"));
        buffer.append(String.format("\t       SignedLong,min,max - a random long in the specified range %n"));
        buffer.append(String.format("\t            where min is inclusive, max is exclusive and %n"));
        buffer.append(String.format("\t            min must be greater than or equal to zero %n"));

        return buffer;
    }
}
