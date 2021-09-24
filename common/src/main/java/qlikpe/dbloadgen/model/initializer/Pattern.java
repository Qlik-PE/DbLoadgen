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

import java.util.ArrayList;
import java.util.List;

/**
 * Generates a string based on the specified pattern.
 * <ul>
 *     <li>integers are specified by '#'</li>
 *     <li>lower case alphabetic characters are specified by '@'</li>
 *     <li>upper case alphabetic characters are specified by '^'</li>
 *     <li>alphanumeric characters are specified by '%'</li>
 *     <li>random characters are specified by '*'</li>
 *     <li>anything else is considered a literal character</li>
 * </ul>
 *
 * Note: Characters that make up the pattern (#, @, %, *) may be used as literals in the
 * pattern by escaping them with a backslash.
 */
public class Pattern  implements Initializer {
    private static final Logger LOG = LogManager.getLogger(Pattern.class);
    private static final String subfieldTypes = "#@^%*";
    private String pattern;
    private int patternLength;
    private List<Initializer> subfields;

    /**
     * Default construtor. Configure() must be called before use.
     */
    public Pattern() { super(); }

    /**
     * Constructor
     * @param pattern the pattern that describes the string to be generated.
     */
    public Pattern(String pattern) {
        configure(pattern);
    }
    /**
     * Configure this instance.
     * @param pattern the pattern that describes the string to be generated.
     */
    public void configure(String pattern) {
        this.pattern = pattern;
        patternLength = pattern.length();
        subfields = new ArrayList<>();
        parsePattern();
    }

    private void parsePattern() {
        char[] patternChars = pattern.toCharArray();
        int patternLength = patternChars.length;
        StringBuilder subPattern = new StringBuilder();
        boolean literal = false;
        boolean escape = false;
        char current;
        char next;
        LOG.debug(String.format("Pattern: %s  Length: %d", pattern, pattern.length()));

        for (int i=0; i < patternLength; i++) {
            current = patternChars[i];
            if (i < patternLength-1)
                next = patternChars[i+1];
            else next = '\0';

            if (current == '\\') {
                current = next;
                i++;
                if (i < patternLength-1)
                    next = patternChars[i+1];
                else next = '\0';
                escape = true;
            }
            subPattern.append(current);
            if (escape || !subfieldTypes.contains(String.valueOf(current))) {
                escape = false;
                literal = true;
                // we are working on a literal and not at EOL
                if ((next != '\0') && !subfieldTypes.contains(String.valueOf(next)))
                    continue;
            }
            if (literal || (current != next)) {
                String type;

                Initializer tmp;
                int length = subPattern.length();
                if (literal) {
                    tmp = new Literal(subPattern.toString());
                    type = "literal";
                    literal = false;
                } else {
                    switch(subPattern.charAt(0)) {
                        case '#':
                            tmp = new PaddedInteger(length);
                            type = "integer";
                            break;
                        case '@':
                            tmp = new RandomString(RandomString.Type.LOWER, length);
                            type = "lower";
                            break;
                        case '^':
                            tmp = new RandomString(RandomString.Type.UPPER, length);
                            type = "upper";
                            break;
                        case '%':
                            tmp = new RandomString(RandomString.Type.ALPHANUMERIC, length);
                            type = "alphanumeric";
                            break;
                        case '*':
                            tmp = new RandomString(RandomString.Type.ALL, length);
                            type = "all";
                            break;
                        default:
                            LOG.error("unrecognized sub-pattern: {}. Defaulting to ALPHANUMERIC.", subPattern);
                            type = "alphanumeric";
                            tmp = new RandomString(RandomString.Type.ALPHANUMERIC, length);
                            break;
                    }
                }
                LOG.debug(String.format("Subpattern: %s  Type: %s Length: %d", subPattern, type, subPattern.length()));
                subfields.add(tmp);
                subPattern.setLength(0);
            }
        }
    }

    /**
     * Generate the next random string based on the pattern.
     * @return the generated string.
     */
    @Override
    public synchronized String nextValue() {
        StringBuilder rval = new StringBuilder(patternLength);

        for(Initializer initializer : subfields) {
            rval.append(initializer.nextValue());
        }
        return rval.toString();
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("Pattern: return return a random string base on the specified pattern%n"));
        buffer.append(String.format("\tUsage: Pattern,<template>%n"));
        buffer.append(String.format("\t\t- integers are specified by '#'%n"));
        buffer.append(String.format("\t\t- lower case alphabetic characters are specified by '@'%n"));
        buffer.append(String.format("\t\t- upper case alphabetic characters are specified by '^'%n"));
        buffer.append(String.format("\t\t- alphanumeric characters are specified by '%c' %n", '%'));
        buffer.append(String.format("\t\t- random characters are specified by '*'%n"));
        buffer.append(String.format("\t\t- anything else is considered a literal character%n"));
        buffer.append(String.format("\t\t- special characters above may be escaped with '\\\\' %n"));

        return buffer;
    }
}
