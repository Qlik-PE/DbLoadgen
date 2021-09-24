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
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A class that describes metadata mapping for a supported data type.
 */
@Getter
@Setter
@NoArgsConstructor
public class DataType {
    private String genericType;
    private String databaseType;
    private String description;
    private boolean precision;  // this type supports a length / precision value.
    private boolean scale;  // this type supports a scale value.

}
