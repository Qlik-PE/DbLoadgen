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

import static java.lang.Integer.max;

/**
 * Generate a string of varying length.
 */
public class VariableString extends FixedString {
    private final int maxLength;
    private final int minLength;

    public VariableString() {
        super();
        maxLength = getLength() + 1;  // exclusive
        minLength = max(1, getLength()/2); // inclusive
    }
    public VariableString(int length) {
        super(length);
        maxLength = getLength() + 1;  // exclusive
        minLength = max(1, getLength()/2);  // inclusive
    }

    @Override
    public String nextValue() {
        int stringLength = ThreadLocalRandom.current().nextInt(minLength, maxLength);

        return super.nextValue().substring(0, stringLength);
    }
}
