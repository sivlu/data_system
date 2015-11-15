//
// Created by Siv Lu on 9/21/15.
//

#ifndef BLUS_CS165_2015_BASE_DATA_STRUCTURE_H
#define BLUS_CS165_2015_BASE_DATA_STRUCTURE_H

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include "utils.h"

/*-----------Start of Definition of Macros------------------- ----*/

#define DEBUG 1 // flag for debug
#define COL_SIZE 10 //number of values in a column
#define DB_SIZE 10//number of tables in a db
#define DATA_PATH "../data/" //default data path to store the data
#define BUF_SIZE 1024 //buffer size for char array
#define PATH_SIZE 256 //buffer size for a file name
#define NAME_SIZE 50//buffer size for a name


/*-----------End of Definition of Macros--------------------------*/


/*-----------Start of Definition of Global Variables---------------*/

/*
 * global var "var_table" is the table we keep track of DSL variables
 */
extern struct var_node* var_table;
/*
 * keep track what db has been created
 */
extern struct db_node* db_table;

/*-----------End of Definition of Global Variables------------------*/


/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator similar to the way IndexType supports
 * additional types.
 **/

/**
typedef enum DataType {
     INT,
     LONG,
     // Others??
} DataType;
**/

/*-----------Start of Type Definition of Enums----------------------*/

/*
 * PosFlag
 * for indication of a position in table or db
 */
typedef enum PosFlag{
    EMPTY,
    FULL,
}PosFlag;

/**
 * IndexType
 * Flag to identify index type.
 * Defines an enum for the different possible column indices.
 * Additional types are encouraged as extra.
 **/
typedef enum IndexType {
    SORTED,
    B_PLUS_TREE,
} IndexType;

/**
 * Defines a comparator flag between two values.
 **/
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
 *     f_b.col = col_b;
 *
 *     // This represents the entire comparator
 *     comparator f;
 *     f.value = 5;
 *     f.type = LESS_THAN | EQUAL;
 *     f.mode = AND;
 *     f.col = col_b;
 *     f.next_comparator = &f_b;
 * For chains of more than two Junctions, left associative: "a | b & c | d"
 * evaluated as "(((a | b) & c) | d)".
 **/
typedef enum Junction {
    NONE,
    OR,
    AND,
} Junction;

typedef enum Aggr {
    MIN,
    MAX,
    SUM,
    AVG,
    CNT,
} Aggr;

typedef enum OperatorType {
//    CREATE,
    DROP,
    SELECT,
    PROJECT,
    HASH_JOIN,
    INSERT,
    DELETE,
    UPDATE,
    AGGREGATE,
} OperatorType;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
    OK,
    ERROR,
} StatusCode;

typedef enum OpenFlag {
    CREATE = 1,
    LOAD = 2,
} OpenFlags;

typedef enum ResultType{
    POS, //positions
    VAL, //values
}ResultType;
/*-----------End of Type Definition of Enums-----------------------*/



/*-----------Start of Type Definition of Struct--------------------*/
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
 * - tb_size, the number of col this table can contain
 * - col_length, the size of the columns in the table, defined as macro.
 **/
typedef struct table {
    char* name;
    size_t col_count;
    column* cols;
    PosFlag* cols_pos;
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
    PosFlag* tables_pos;
    size_t db_size;
} db;

/* status declares an error code and associated message
 *
 */
typedef struct status {
    StatusCode code;
    char error_message[BUF_SIZE];
} status;

/**
 * comparator
 * A comparator defines one or more comparisons.
 * See the example in Junction
 **/
typedef struct comparator {
    int p_val;
    column *col;
    ComparatorType type;
    Junction mode;
    struct comparator *next_comparator;
} comparator;

typedef struct result {
    size_t num_tuples;
    int *payload;
    ResultType type;
} result;


typedef struct query_org{
    OperatorType type; //type of the function
    char* leftside; //variable name on the left side of "="
    char* rightside; //array of arguments
}query_org;

typedef struct val_pos{
    int val;
    int pos;
}val_pos;

/**
 * db_operator
 * The db_operator defines a database operation.  Generally, an operation will
 * be applied on column(s) of a table (SELECT, PROJECT, AGGREGATE) while other
 * operations may involve multiple tables (JOINS). The OperatorType defines
 * the kind of operation.
 *
 * In UNARY operators that only require a single table, only the variables
 * related to table1/column1 will be used.
 * In BINARY operators, the variables related to table2/column2 will be used.
 *
 * If you are operating on more than two tables, you should rely on separating
 * it into multiple operations (e.g., if you have to project on more than 2
 * tables, you should select in one operation, and then create separate
 * operators for each projection).
 *
 * Example query:
 * SELECT a FROM A WHERE A.a < 100;
 * db_operator op1;
 * op1.table1 = A;
 * op1.column1 = b;
 *
 * filter f;
 * f.value = 100;
 * f.type = LESS_THAN;
 * f.mode = NONE;
 *
 * op1.comparator = f;
 **/
typedef struct db_operator {
    // Flag to choose operator
    OperatorType type;

    // Used for every operator
    table** tables;
    column** columns;

    // Intermediaties used for PROJECT, DELETE, HASH_JOIN
    int *pos1;
    // Needed for HASH_JOIN
    int *pos2;

    // For insert/delete operations, we only use value1;
    // For update operations, we update value1 -> value2;
    int *value1;
    int *value2;

    // This includes several possible fields that may be used in the operation.
    Aggr agg;
    comparator* c;

} db_operator;

/*
 * db_node to form a linked list
 * this_db: points to a db
 * next: points to next node
 */
typedef struct db_node{
    db* this_db;
    struct db_node* next;
}db_node;

/*
 * var_node to form a linked list to keep track of
 * variables used in DSL
 */
typedef struct var_node{
    char* var_name;//variable name found in DSL query
    char* var_type;//type used to cast var_object
    void* var_object;//pt to object , type can be db, table, col, value array
    struct var_node* next_var; //pt to next var_node
}var_entry;

typedef struct file_node{
    char* filename;
    struct file_node* next;
}file_node;


/*-----------End of Type Definition of Struct-------------------*/


#endif //BLUS_CS165_2015_BASE_DATA_STRUCTURE_H
