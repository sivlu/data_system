//
// Created by Siv Lu on 9/21/15.
//

#ifndef BLUS_CS165_2015_BASE_DATA_STRUCTURE_H
#define BLUS_CS165_2015_BASE_DATA_STRUCTURE_H

#include <stdlib.h>

#define VAR_TABLE_SIZE 100 //number of variables we keep track
#define COL_SIZE 100 //number of values in a column
#define DB_SIZE 10//number of tables in a db


//global var "var_table" is the table we keep track of "var_entry" records
extern struct var_entry var_table[VAR_TABLE_SIZE];

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/
/**
typedef enum DataType {
     INT,
     LONG,
     // Others??
} DataType;
**/

/*
 * FlagType
 */
typedef enum FlagType{
    EMPTY,
    FULL,
}FlagType;

/**
 * IndexType
 * Flag to identify index type. Defines an enum for the different possible column indices.
 * Additonal types are encouraged as extra.
 **/
typedef enum IndexType {
    SORTED,
    B_PLUS_TREE,
} IndexType;

/**
 * column_index
 * Defines a general column_index structure, which can be used as a sorted
 * index or a b+-tree index.
 * - type, the column index type (see enum index_type)
 * - index, a pointer to the index structure. For SORTED, this points to the
 *       start of the sorted array. For B+Tree, this points to the root node.
 *       You will need to cast this from void* to the appropriate type when
 *       working with the index.
 **/
typedef struct column_index {
    IndexType type;
    void* index;
} column_index;

/**
 * column
 * Defines a column structure, which is the building block of our column-store.
 * Operations can be performed on one or more columns.
 * - name, the string associated with the column. Column names must be unique
 *       within a table, but columns from different tables can have the same
 *       name.
 * - data, this is the raw data for the column. Operations on the data should
 *       be persistent.
 * - index, this is an [opt] index built on top of the column's data.
 * - row_count, keeps track of next available row
 *
 * NOTE: We do not track the column length in the column struct since all
 * columns in a table should share the same length. Instead, this is
 * tracked in the table (length).
 **/
typedef struct column {
    char* name;
    int* data;
    int row_count; //keep it as for milestone 1, may need to change
    column_index *index;
} column;

/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * - name, the name associated with the table. Table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - cols_pos, pointer to an array of flags, determining if a col is taken or not
 * - cols, this is the pointer to an array of columns contained in the table.
 * - tb_size, the number of col this table can contain, defined as a macro
 * - col_length, the size of the columns in the table.
 **/
typedef struct table {
    char* name;
    size_t col_count;
    column* cols;
    FlagType* cols_pos;
    size_t tb_size;
    size_t col_length;

} table;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - table_count: the number of tables in the database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_pos: the pointer to the array of int (0 or 1) to keep track
 *               of which pos in tables array is taken. 1 is occupied, 0 is empty
 * - db_size: number of tables this db can contain at most, defined as a macro
 **/
typedef struct db {
    char* name;
    size_t table_count;
    table* tables;
    FlagType* tables_pos;
    size_t db_size;
} db;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
    /* The operation completed successfully */
            OK,
    /* There was an error with the call.
    */
            ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct status {
    StatusCode code;
    char* error_message;
} status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
} ComparatorType;

/**
 * A Junction defines the relationship between comparators.
 * In cases where we have more than one comparator, e.g. A.a <= 5 AND A.b > 3,
 * A NONE Junction defines the END of a comparator junction.
 *
 * Using the comparator struct defined below, we would represent our example using:
 *     // This represents the sub-component (A.b > 3)
 *     comparator f_b;
 *     f_b.p_val = 3; // Predicate values
 *     f_b.type = GREATER_THAN;
 *     f_b.mode = NONE;
 *
 *     // This represents the entire comparator
 *     comparator f;
 *     f.value = 5;
 *     f.mode = LESS_THAN | EQUAL;
 *     f.next_comparator = &f_b;
 * For chains of more than two Juntions, left associative: "a | b & c | d"
 * evaluated as "(((a | b) & c) | d)".
 **/

typedef enum Junction {
    NONE,
    OR,
    AND,
} Junction;

/**
 * comparator
 * A comparator defines one or more comparisons.
 * See the example in Junction
 **/
typedef struct comparator {
    int p_val;
    column *col;
    ComparatorType type;
    struct comparator *next_comparator;
    Junction mode;
} comparator;

typedef struct result {
    size_t num_tuples;
    int *payload;
} result;

typedef enum Aggr {
    MIN,
    MAX,
    SUM,
    AVG,
    CNT,
} Aggr;

typedef enum OperatorType {
    CREATE,
    DROP,
    SELECT,
    PROJECT,
    HASH_JOIN,
    INSERT,
    DELETE,
    UPDATE,
    AGGREGATE,
} OperatorType;

typedef struct query_org{
    OperatorType type; //type of the function
    char* leftside; //variable name on the left side of "="
    char* rightside; //array of arguments
}query_org;

typedef struct var_entry{
    char* var_name;//variable name found in DSL query
    void* object;//pt to object , type can be db, table, col, value array
}var_entry;

#endif //BLUS_CS165_2015_BASE_DATA_STRUCTURE_H
