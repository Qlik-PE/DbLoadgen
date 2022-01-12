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

import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate a number with the required precision and scale.
 */
public class RandomDecimal implements Initializer {
    private int precision;
    private int scale;
    private long min;
    private long max;
    private DecimalFormat df;


    /**
     * Generate numbers with default values of precision,scale, min, and max:
     * <ul>
     *     <li>precision = 10</li>
     *     <li>scale = 2</li>
     *     <li>min = 0</li>
     *     <li>max = 1^8</li>
     * </ul>
     */
    public RandomDecimal() {
        configure(10, 2, 0, (long) Math.pow(10, (10-2)));
    }

    /**
     * Generate numbers with the specified precision and scale. The range will be from
     * 0 to 1^(precision-scale).
     *
     * @param precision the total number of digits in the number
     * @param scale the number of digits to the right of the decimal point.
     */
    public RandomDecimal(int precision, int scale) {
        if (scale > precision)
            throw new RuntimeException(String.format("invalid precision/scale specified: precision(%d) scale(%d)",
                    precision, scale));

        configure(precision, scale, 0, (long) Math.pow(10, (precision-scale)));
    }

    /**
     * Generate numbers with the specified precision and scale within the specified range.
     *
     * @param precision the total number of digits in the number
     * @param scale the number of digits to the right of the decimal point.
     * @param min the minimum value to generate
     * @param max the maximum value to generate
     */
    public RandomDecimal(int precision, int scale, long min, long max) {
        if (scale > precision)
            throw new RuntimeException(String.format("invalid precision/scale specified: precision(%d) scale(%d)",
                    precision, scale));
        if (min > max)
            throw new RuntimeException(String.format("invalid range specified: min(%d) max(%d)", min, max));

        configure(precision, scale, min, max);
    }

    public void configure (int precision, int scale) {
        configure(precision, scale, 0, (long)Math.pow(10, (precision-scale)));
    }

    public void configure (int precision, int scale, long min, long max) {
        this.precision = precision;
        this.scale = scale;
        this.min = min;
        this.max = max;
        this.df = new DecimalFormat(getPattern());
        //System.out.printf("RandomDecimal: precision(%d) scale(%d) min(%d) max(%d) %n", precision, scale, min, max);
    }

    /**
     * Generate the format pattern for this Number.
     * @return a format pattern as a String.
     */
    private String getPattern() {
        String pattern = "";
        for (int i = 0; i < (precision - scale - 1); i++) {
            pattern += "#";
        }
        if (scale > 0) {
            pattern += "0.0";
            for(int i = 0; i < (scale-1); i++) {
                pattern += "#";
            }
        } else {
            pattern += "0";
        }
        //System.out.printf("RandomString: pattern(%s)%n", pattern);
        return pattern;
    }

    /**
     * Get the next random value.
     * @return the next random value.
     */
    @Override
    public synchronized String nextValue() {
        double value = ThreadLocalRandom.current().nextDouble(min, max);
        return df.format(value);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("RandomDecimal: return a random decimal number%n"));
        buffer.append(String.format("\tUsage: RandomDecimal - defaults to precision(8), scale(2), min(0), max(max value)%n"));
        buffer.append(String.format("\t       RandomDecimal,<precision>,<scale> - defaults to min(0), max(max value)%n"));
        buffer.append(String.format("\t       RandomDecimal,<precision>,<scale>,<min>,<max>%n"));

        return buffer;
    }
}
