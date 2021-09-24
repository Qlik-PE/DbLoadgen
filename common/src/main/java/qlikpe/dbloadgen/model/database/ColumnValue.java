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
package qlikpe.dbloadgen.model.database;

import lombok.Getter;
import lombok.Setter;

/**
 * Manages a column name/value pair.
 */
@Getter
@Setter
public class ColumnValue {
    private String columnName;
    private String value;

    /**
     * Store a column/value pair.
     * @param columnName the name of the column.
     * @param value the column's value.
     */
    public ColumnValue(String columnName, String value) {
        this.columnName = columnName;
        this.value = value;
    }
}
