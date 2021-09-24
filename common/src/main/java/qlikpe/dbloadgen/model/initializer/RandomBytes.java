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


import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.binary.Hex;


public class RandomBytes extends FixedString {
    public RandomBytes() {
        super();
    }

    public RandomBytes(int length) {
        super(length);
    }

    @Override
    public String nextValue() {
        char[] hex =  Hex.encodeHex(super.nextValue().getBytes(StandardCharsets.UTF_8));

        return String.valueOf(hex);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("RandomBytes: generate an array of bytes to assign to a binary column value.%n"));
        buffer.append(String.format("\tRelatively efficient but not terribly random.%n"));
        buffer.append(String.format("\tNot suitable for use in a key column.%n"));
        buffer.append(String.format("\tUsage: RandomBytes - generate a string of the default length of 10%n"));
        buffer.append(String.format("\t       RandomBytes,<length> - generate a string of the specified length%n"));

        return buffer;
    }
}
