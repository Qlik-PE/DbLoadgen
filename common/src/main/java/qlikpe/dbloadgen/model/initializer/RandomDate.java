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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates a random date of the form YYYY-MM-DD.
 */
public class RandomDate implements Initializer {
    private final long startDate;
    private final long endDate;


    public RandomDate() {
        int tenYears = 10 * 365;
        endDate = LocalDate.now().toEpochDay();
        startDate = endDate - tenYears;
    }

    @Override
    public String nextValue() {
        long randomDate = ThreadLocalRandom.current().nextLong(startDate, endDate);
        return LocalDate.ofEpochDay(randomDate).format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("RandomDate: return a random date from the last 10 years in the form YYYY-MM-DD%n"));
        buffer.append(String.format("\tUsage: RandomDate%n"));

        return buffer;
    }
}
