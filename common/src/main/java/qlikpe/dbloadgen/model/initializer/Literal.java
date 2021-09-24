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

/**
 * Returns a literal value each time it is called.
 */
public class Literal implements Initializer {
    String value;

    /**
     * Default constructor.
     * config() must be called before use.
     */
    public Literal() { super(); }

    /**
     * Construct the literal.
     * @param value the value of this literal.
     */
    public Literal(String value) {
        super();
        configure(value);
    }

    public void configure(String value) {
        this.value = value;
    }

    /**
     * The value returned is the same for each call.
     * @return the value of this literal.
     */
    @Override
    public synchronized String nextValue() {
        return value;
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("Literal: always return the specified literal%n"));
        buffer.append(String.format("\tUsage: Literal,<string> - the string to return%n"));

        return buffer;
    }

}
