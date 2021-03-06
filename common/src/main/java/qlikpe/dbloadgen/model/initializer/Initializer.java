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
 * Public interface implemented by classes that are used to
 * generate values for columns when inserting rows.
 */
public interface Initializer {
        /**
         * Generate and return a random column value.
         * @return a generated value as a String.
         */
        String nextValue();

        /**
         * Generates "how to" usage information for this initializer,
         * appending to the input buffer if it is not null.
         * @return an instance of StringBuilder containing the usage info.
         */
        StringBuilder getHelp(StringBuilder buffer);
}
