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
 * Generates a random datetime of the form YYYY-MM-DD hh::mm:ss.
 */
public class DateTime implements Initializer {
    private final RandomDate randomDate = new RandomDate();
    private final RandomTime randomTime = new RandomTime();


    public DateTime() {
        int tenYears = 10 * 365;
    }

    @Override
    public String nextValue() {
        String time = randomTime.nextValue();
        String date = randomDate.nextValue();
        return String.format("%s %s", date, time);
    }

    @Override
    public StringBuilder getHelp(StringBuilder buffer) {
        if (buffer == null)
            buffer = new StringBuilder();
        buffer.append(String.format("DateTime: return a random date time of the form YYYY-MM-DD HH:mm:ss%n"));
        buffer.append(String.format("\tUsage: DataTime%n"));

        return buffer;
    }
}
