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
 * Generate a random string of the specified type and length.
 *
 * Type is an enum, Type:
 *
 * <ul>
 *     <li>Type.UPPER: generate an uppercase string</li>
 *     <li>Type.LOWER: generate a lowercase string</li>
 *     <li>Type.ALPHA: generate a string containing uppercase and lowercase letters</li>
 *     <li>Type.NUMBERS: generate a string containing only numbers</li>
 *     <li>Type.ALL: generate a string containing all characters.</li>
 * </ul>
 */
public class RandomString implements Initializer {
    public enum Type {UPPER, LOWER, ALPHA, ALPHANUMERIC, NUMERIC, ALL};
    private final static String upper_case = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private final static String lower_case = "abcdefghijklmnopqrstuvwxyz";
    private final static String numbers = "1234567890";
    private final static String special = "!?~@#-_+";

    private int length;
    private int setLength;
    private String charSet;

    /**
     * Default constructor. Configure() must be called before use.
     */
    public RandomString() {
        super();
        configure(Type.ALPHANUMERIC, 10);
    }

    /**
     * Constructor.
     * @param type UPPER, LOWER, ALPHA, NUMERIC, ALL
     * @param length the length of the generated string
     */
    public RandomString(Type type, int length) {
        super();
        configure(type, length);
    }
    /**
     * Configure the instance instance.
     *
     * @param type UPPER, LOWER, ALPHA, NUMERIC, ALL
     * @param length the length of the generated string
     */
    public void configure (Type type, int length) {
        this.length = length;
        switch(type) {
            case UPPER: charSet = upper_case; break;
            case LOWER: charSet = lower_case; break;
            case ALPHA: charSet = upper_case + lower_case; break;
            case NUMERIC: charSet = numbers; break;
            case ALPHANUMERIC: charSet = upper_case + lower_case + numbers; break;
            case ALL:
            default:
                charSet = upper_case + lower_case + numbers + special; break;
        }
        this.setLength = charSet.length();
    }

    public Type convertType(String type) {
        Type enumType;
        switch(type) {
            case "UPPER":
                enumType = Type.UPPER;
                break;
            case "LOWER":
                enumType = Type.LOWER;
                break;
            case "ALPHA":
                enumType = Type.ALPHA;
                break;
            case "NUMERIC":
                enumType = Type.NUMERIC;
                break;
            case "ALPHANUMERIC":
                enumType = Type.ALPHANUMERIC;
                break;
            case "ALL":
            default:
                enumType = Type.ALL;
                break;
        }
       return enumType;
    }

    @Override
    public synchronized String nextValue() {
        StringBuilder rval = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            rval.append(charSet.charAt(ThreadLocalRandom.current().nextInt(0, setLength)));
        }
        return rval.toString();
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("RandomString: return a random string. All characters in the string are%n"));
        buffer.append(String.format("\trandomly selected. This is suitable for use in key columns.%n"));
        buffer.append(String.format("\tUsage: RandomString - generate an alphanumeric string of length 10. %n"));
        buffer.append(String.format("\t       RandomString,<type>,<length> - generate a string of the %n"));
        buffer.append(String.format("\t            specified length where type is one of %n"));
        buffer.append(String.format("\t            UPPER, LOWER, ALPHA, NUMERIC, ALPHANUMERIC,  or ALL%n"));

        return buffer;
    }
}
