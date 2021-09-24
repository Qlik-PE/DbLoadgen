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

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generate a random time of day in the format HH:MM:SS.
 */
public class RandomTime implements Initializer {
    private final int startSeconds;
    private final int endSeconds;

    public RandomTime() {
        startSeconds = LocalTime.MIN.toSecondOfDay();
        endSeconds = LocalTime.MAX.toSecondOfDay();
    }

    @Override
    public String nextValue() {
        int randomTime = ThreadLocalRandom.current().nextInt(startSeconds, endSeconds);
        return LocalTime.ofSecondOfDay(randomTime).format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("RandomTime: return a random time of day in the form HH:mm:ss%n"));
        buffer.append(String.format("\tUsage: RandomTime%n"));

        return buffer;
    }
}
