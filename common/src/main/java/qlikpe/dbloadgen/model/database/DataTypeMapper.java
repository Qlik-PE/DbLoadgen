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


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * A class that maps "generic" data types supported by DbLoadGen to the
 * actual types supported by the database.
 */
public class DataTypeMapper {
    private static final Logger LOG = LogManager.getLogger(DataTypeMapper.class);
    private static final String defaultType = "VARCHAR(50)";
    private final Map<String, DataType> typeMap;
    private final Database database;

    public DataTypeMapper(Database database) {
        this.database = database;
        typeMap = new HashMap<>();
        setDefaultTypeMap("CHAR", "CHAR", true, false, "A fixed length character string");
        setDefaultTypeMap("NCHAR", "NCHAR", true, false, "A fixed length double-byte character string");
        setDefaultTypeMap("VARCHAR", "VARCHAR", true, false, "A variable length character string");
        setDefaultTypeMap("NVARCHAR", "NVARCHAR", true, false, "A variable length double-byte character string");
        setDefaultTypeMap("BOOLEAN", "BOOLEAN", false, false, "A Boolean value");
        setDefaultTypeMap("BYTES", "BINARY", true, false, "A fixed length binary data value");
        setDefaultTypeMap("VARBYTES", "VARBINARY", true, false, "A variable length binary data value");
        setDefaultTypeMap("DATE", "DATE", false, false, "A date value: YYYY-MM-DD");
        setDefaultTypeMap("TIME", "TIME", false, false, "A time value: HH:MM:SS");
        setDefaultTypeMap("DATETIME", "DATETIME", false, false, "A timestamp value: YYYY-MM-DD HH:MM:SS.sss");
        setDefaultTypeMap("INT1", "TINYINT", false, false, "A one-byte, signed integer");
        setDefaultTypeMap("INT2", "SMALLINT", false, false, "A two-byte, signed integer");
        setDefaultTypeMap("INT3", "MEDIUMINT", false, false, "A three-byte, signed integer");
        setDefaultTypeMap("INT4", "INTEGER", false, false, "A four-byte, signed integer");
        setDefaultTypeMap("INT8", "BIGINT", false, false, "An eight-byte, signed integer");
        setDefaultTypeMap("NUMERIC", "NUMERIC", true, true, "An exact numeric value with a fixed precision and scale");
        setDefaultTypeMap("FLOAT", "FLOAT", false, false, "A single-precision floating-point value");
        setDefaultTypeMap("DOUBLE", "DOUBLE", false, false, "A double-precision floating-point value");
        setDefaultTypeMap("UINT1", "TINYINT UNSIGNED", false, false, "A one-byte, unsigned integer");
        setDefaultTypeMap("UINT2", "SMALLINT UNSIGNED", false, false, "A two-byte, unsigned integer");
        setDefaultTypeMap("UINT3", "MEDIUMINT UNSIGNED", false, false, "A three-byte, unsigned integer");
        setDefaultTypeMap("UINT4", "INTEGER UNSIGNED", false, false, "A four-byte, unsigned integer");
        setDefaultTypeMap("UINT8", "BIGINT UNSIGNED", false, false, "An eight-byte, unsigned integer");
        setDefaultTypeMap("BLOB", "BLOB", false, false, "Binary Large Object");
        setDefaultTypeMap("CLOB", "TEXT", false, false, "Character Large Object");
        setDefaultTypeMap("NCLOB", "TEXT", false, false, "Native Character Large Object");
    }

    /**
     * Set the default mapping of generic type to database type. The idea here is that we
     * will only need to override types that differ in the database "dialect" rather than
     * having to specify everything in every database.
     * @param genericType the generic type specified in the table yaml definitions
     * @param databaseType the type to map the generic type to.
     * @param precision a boolean indicating that this type supports precision/length.
     * @param scale a boolean indicating that this type supports a value for "scale".
     * @param description a description of the generic type.
     */
    public void setDefaultTypeMap(String genericType, String databaseType, boolean precision, boolean scale, String description) {
        DataType dataType = new DataType();
        dataType.setGenericType(genericType);
        dataType.setDatabaseType(databaseType);
        dataType.setPrecision(precision);
        dataType.setScale(scale);
        dataType.setDescription(description);

        typeMap.put(genericType, dataType);
    }

    /**
     * Gets the mapped type for the database.
     * @param genericType the type specified in the table's yaml definition.
     * @return a type specific for the database we are working with.
     */
    public String getDatabaseType(String genericType) {
        return typeMap.get(genericType).getDatabaseType();
    }

    /**
     * Gets the instance of Database.
     * @return a Database.
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * Override the default database type with one specific to the database we are targeting.
     * This will be called from the database dialect for the database.
     * @param genericType the generic type specified in the table's yaml definition.
     * @param databaseType the new database type.
     */
    public void overrideDefaultDatabaseType(String genericType, String databaseType) {
        typeMap.get(genericType).setDatabaseType(databaseType);
    }

    /**
     * Checks whether the generic type specified is a valid generic type.
     * @param genericType the generic type specified in the table's yaml definition.
     * @return true if it is a valid type, false otherwise.
     */
    public boolean validType(String genericType) {
        return typeMap.containsKey(genericType);
    }

    /**
     * Maps the generic type specified for the column to a data type specific to this database.
     * @param genericType the column's generic type.
     * @param precision the specified precision or -1 if not specified.
     * @param scale the specified scale, or -1 if not specified.
     * @return a database type for this column.
     */
    public String mapType(String genericType, int precision, int scale) {
        String databaseType;

        if (validType(genericType)) {
            DataType dataType = typeMap.get(genericType);
            if (dataType.isPrecision() && (precision != -1)) {
                if (dataType.isScale()) {
                    databaseType = String.format("%s(%d,%d)", dataType.getDatabaseType(), precision, scale);
                } else {
                    databaseType = String.format("%s(%d)", dataType.getDatabaseType(), precision);
                }

            } else databaseType = dataType.getDatabaseType();

        } else {
            LOG.warn("invalid type {} specified. Defaulting to {}.", genericType, defaultType);
            databaseType = defaultType;
        }
        return databaseType;
    }

    public String getDefaultInitializer(String genericType, int precision, int scale) {
        String initializer;
        switch(genericType) {
            case "CHAR":
            case "NCHAR":
                initializer = String.format("FixedString,%d", precision);
                break;
            case "VARCHAR":
            case "NVARCHAR":
                initializer = String.format("VariableString,%d", precision);
                break;
            case "BOOLEAN":
                initializer = "LiteralArray,true,false";
                break;
            case "BYTES":
            case "VARBYTES":
                initializer = String.format("RandomBytes,%d", precision);
                break;
            case "DATE":
                initializer = "RandomDate";
                break;
            case "TIME":
                initializer = "RandomTime";
                break;
            case "DATETIME":
                initializer = "DateTime";
                break;
            case "INT1":
                initializer = "SignedInteger,-125,125";
                break;
            case "INT2":
                initializer = "SignedInteger,-32765,32765";
                break;
            case "INT3":
                initializer = "SignedInteger,-8388605,8388605";
                break;
            case "INT4":
                initializer = "SignedInteger,-1063741820,1063741820";
                break;
            case "INT8":
                initializer = "SignedLong,-4223372036854775800,4223372036854775800";
                break;
            case "DECIMAL":
            case "NUMERIC":
                initializer = String.format("RandomDecimal,%d,%d", precision, scale);
                break;
            case "FLOAT":
                initializer = String.format("RandomDecimal,%d,%d", 23, 7);
                break;
            case "DOUBLE":
                initializer = String.format("RandomDecimal,%d,%d", 53, 7);
                break;
            case "UINT1":
                if (database.getSupportsUnsignedInts())
                    initializer = "UnsignedInteger,0,250";
                else initializer = "SignedInteger,0,120";
                break;
            case "UINT2":
                if (database.getSupportsUnsignedInts())
                    initializer = "UnsignedInteger,0,65530";
                else initializer = "SignedInteger,0,32760";
                break;
            case "UINT3":
                if (database.getSupportsUnsignedInts())
                    initializer = "UnsignedInteger,0,16777210";
                else initializer = "SignedInteger,0,8388600";
                break;
            case "UINT4":
                // needs to be long to hold an int this big as java doesn't really
                // understand "unsigned".
                if (database.getSupportsUnsignedInts()) {
                    initializer = "UnsignedLong,0,4294967290";
                } else initializer = "SignedInteger,0,2147483640";
                break;
            case "UINT8":
                if (database.getSupportsUnsignedInts())
                    initializer = "UnsignedLong,0,9223372036854775800";
                else initializer = "SignedLong,0,9223372036854775800";
                break;
            case "BLOB":
                initializer = "RandomBytes,4096";
                break;
            case "CLOB":
            case "NCLOB":
                initializer = "FixedString,4096";
                break;
            default:
                LOG.warn("getDefaultInitializer(): unrecognized generic type: {}. Defaulting to FixedString", genericType);
                initializer = "FixedString,50";
                break;

        }
        return initializer;
    }

    /**
     * Dump the generic types and their descriptions to the standard output.
     */
    public void describeGenericTypes() {
        for (Map.Entry<String, DataType> entry : typeMap.entrySet()) {
            System.out.printf("%-10s : %s", entry.getKey(), entry.getValue().getDescription());
        }
    }
}
