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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Return a randomly selected literal from the array
 * specified at construction time.
 */
public class LiteralArray implements Initializer {
    private final ArrayList<String> values;
    private int size;

    /**
     * Default Constructor
     */
    public LiteralArray() { values = new ArrayList<>(); }

    /**
     * The constructor.
     * @param list an array of strings
     */
    public void configure(String[] list) {
        // skip the first value, which will be "LiteralArray"
        values.addAll(Arrays.asList(list).subList(1, list.length));

        size = values.size();
    }

    @Override
    public String nextValue() {
        return values.get(ThreadLocalRandom.current().nextInt(size));
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("LiteralArray: return a random value from the specified array%n"));
        buffer.append(String.format("\tUsage: LiteralArray,<value 1>,<value 2>,...,<value N>%n"));

        return buffer;
    }

}
