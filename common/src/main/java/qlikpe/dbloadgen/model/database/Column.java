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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import qlikpe.dbloadgen.model.initializer.*;
import qlikpe.dbloadgen.model.workload.WorkloadInitializationException;

import java.lang.reflect.InvocationTargetException;


/**
 * Manages the metadata associated with a column. This data is parsed from the
 * dataset/tableName.yml files.
 */
@Getter
@Setter
public class Column {
    private final static Logger LOG = LogManager.getLogger(Column.class);
    private String name;
    private String type;         // the generic column type from the table.yml file
    private String databaseType; // the actual column type for this database dialect.
    private boolean nullable;
    private String initializer;
    private Initializer randomizer = null;

    private int precision;
    private int scale;

    /**
     * The constructor.
     */
    public Column() {
    }

    /**
     * Finish configuring this column before we execute a test.
     * This method parses the specified generic type to get the
     * type, precision, and scale information. Then it sets the
     * actual database type that will be used in the SQL and
     * creates the initializer that will randomize data for this
     * column.
     * @param dataTypeMapper a class that maps the generic to the physical types.
     */
    public void configureColumn(DataTypeMapper dataTypeMapper) {
        String genericType;
        int open = type.indexOf('(');
        int close = type.indexOf(')');
        int comma = type.indexOf(',');

        if (open != -1) {
            genericType = type.toUpperCase().substring(0, open);
            if (comma != -1) {
                precision = Integer.parseInt(type.substring(open+1, comma));
                scale = Integer.parseInt(type.substring(comma+1, close));
            } else {
                precision = Integer.parseInt(type.substring(open+1, close));
                scale = -1;
            }
        } else {
            genericType = type.toUpperCase();
            precision = -1;
            scale = -1;
        }
        databaseType = dataTypeMapper.mapType(genericType, precision, scale);
        if (initializer == null) {
            initializer = dataTypeMapper.getDefaultInitializer(genericType, precision, scale);
        } else {
            if (!dataTypeMapper.getDatabase().getSupportsUnsignedInts()) {
                if (initializer.contains("Unsigned")) {
                    // this database doesn't support "Unsigned" integers, so convert the type.
                    String orig = initializer;
                    initializer = dataTypeMapper.getDefaultInitializer(genericType, precision, scale);
                    LOG.warn("Unsigned Integers are not supported on this platform. Converting type {} to {}",
                            orig, initializer);
                }
            }
        }
        try {
            configureInitializer();
        } catch (WorkloadInitializationException e) {
            LOG.error("failed to configure randomizer: {}", e.getMessage());
        }
    }


    /**
     * Configure the initializer for this column.
     * @throws WorkloadInitializationException on a configuration error.
     */
    private void configureInitializer() throws WorkloadInitializationException {
        String[] initializerArgs = initializer.split(",");
        String initializerType = initializerArgs[0];
        int argCount = initializerArgs.length - 1;
        int precision, scale, min, max, length;
        long minl, maxl;
        RandomString.Type type;


        String className = "qlikpe.dbloadgen.model.initializer." + initializerType;
        try {
            Class<?> clazz = Class.forName(className);
            randomizer = (Initializer)clazz.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            String message = String.format("Initializer not found: %s", className);
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        } catch (InstantiationException e) {
            String message = String.format("Failed to instantiate initializer: %s", className);
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        } catch (IllegalAccessException e) {
            String message = String.format("Illegal access exception for initializer: %s", className);
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        } catch (NoSuchMethodException e) {
            String message = String.format("No such method: %s", className);
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        } catch (InvocationTargetException e) {
            String message = String.format("Invocation target exception: %s", className);
            LOG.error(message, e);
            throw new WorkloadInitializationException(message);
        }

        switch(initializerType) {
            case "Literal": ((Literal)randomizer).configure(initializerArgs[1]); break;
            case "LiteralArray": ((LiteralArray)randomizer).configure(initializerArgs); break;
            case "FixedString":
                if (argCount == 1)
                    ((FixedString)randomizer).configure(Integer.parseInt(initializerArgs[1]));
                break;
            case "VariableString":
                if (argCount == 1)
                    ((VariableString)randomizer).configure(Integer.parseInt(initializerArgs[1]));
                break;
            case "RandomBytes":
                if (argCount == 1)
                    ((RandomBytes)randomizer).configure(Integer.parseInt(initializerArgs[1]));
                break;
            case "Pattern": ((Pattern)randomizer).configure(initializerArgs[1]); break;
            case "RandomDecimal":
                switch(argCount) {
                    case 4:
                        precision = Integer.parseInt(initializerArgs[1]);
                        scale = Integer.parseInt(initializerArgs[2]);
                        min = Integer.parseInt(initializerArgs[3]);
                        max = Integer.parseInt(initializerArgs[4]);
                        ((RandomDecimal) randomizer).configure(precision, scale, min, max);
                        break;
                    case 2:
                        precision = Integer.parseInt(initializerArgs[1]);
                        scale = Integer.parseInt(initializerArgs[2]);
                        min = 0;
                        max = 1 ^ (precision - scale);
                        ((RandomDecimal) randomizer).configure(precision, scale, min, max);
                        break;
                    default:
                        break;
                }
                break;
            case "UnsignedInteger":
                switch(argCount) {
                    case 2:
                        min = Integer.parseUnsignedInt(initializerArgs[1]);
                        max = Integer.parseUnsignedInt(initializerArgs[2]);
                        ((UnsignedInteger) randomizer).configure(min, max);
                        break;
                    case 1:
                        max = Integer.parseUnsignedInt(initializerArgs[1]);
                        ((UnsignedInteger) randomizer).configure(0, max);
                        break;
                    default:
                        break;
                }
                break;
                case "SignedInteger":
                    switch(argCount) {
                        case 2:
                            min = Integer.parseInt(initializerArgs[1]);
                            max = Integer.parseInt(initializerArgs[2]);
                            ((SignedInteger) randomizer).configure(min, max);
                            break;
                        case 1:
                            max = Integer.parseInt(initializerArgs[1]);
                            ((SignedInteger) randomizer).configure(0, max);
                            break;
                        default:
                            break;
                    }
                    break;
                case "UnsignedLong":
                    switch(argCount) {
                        case 2:
                            minl = Long.parseUnsignedLong(initializerArgs[1]);
                            maxl = Long.parseUnsignedLong(initializerArgs[2]);
                            ((UnsignedLong) randomizer).configure(minl, maxl);
                            break;
                        case 1:
                            maxl = Long.parseUnsignedLong(initializerArgs[1]);
                            ((UnsignedLong) randomizer).configure(0, maxl);
                            break;
                        default:
                            break;
                    }
                break;
            case "SignedLong":
                switch(argCount) {
                    case 2:
                        minl = Long.parseLong(initializerArgs[1]);
                        maxl = Long.parseLong(initializerArgs[2]);
                        ((SignedLong) randomizer).configure(minl, maxl);
                        break;
                    case 1:
                        maxl = Long.parseLong(initializerArgs[1]);
                        ((SignedLong) randomizer).configure(0, maxl);
                        break;
                    default:
                        break;
                }
                break;
            case "RandomString":
                if (argCount == 1) {
                    type = RandomString.Type.ALPHANUMERIC;
                    length = Integer.parseInt(initializerArgs[1]);
                } else if (argCount == 2) {
                    type = ((RandomString) randomizer).convertType(initializerArgs[1]);
                    length = Integer.parseInt(initializerArgs[2]);
                } else {
                    type = RandomString.Type.ALPHANUMERIC;
                    length = 10;
                }
                ((RandomString) randomizer).configure(type, length);
                break;
            case "PaddedInteger":
                if (argCount == 1)
                    ((PaddedInteger)randomizer).configure(Integer.parseInt(initializerArgs[1]));
                break;
            case "DateTime":
            case "RandomDate":
            case "RandomTime":
            case "SSN":
            default:
                break;
        }
    }

    /**
     * Generates a random value for this column.
     * @return a random value as a String.
     */
    public String nextValue()  { return randomizer.nextValue(); }

}
