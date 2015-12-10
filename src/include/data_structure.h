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
#include <unistd.h>
#include "utils.h"

/*-----------Start of Definition of Macros------------------- ----*/

#define DEBUG 1 // flag for debug
#define COL_SIZE 1000000 //number of values in a column, need to be fixed
#define DB_SIZE 10//number of tables in a db
#define DATA_PATH "../data/" //default data path to store the data
#define BUF_SIZE 1024 //buffer size for char array
#define PATH_SIZE 256 //buffer size for a file name
#define NAME_SIZE 50//buffer size for a name
#define NUM_THREAD 2 //number of threads used, need to fix later
#define PAGE_SIZE 100 //number of int in a "page" in L1, need to fix later
#define NUM_PARTITION 2 //number of partitions for partition in hash join


/*-----------End of Definition of Macros--------------------------*/


/*-----------Start of Definition of Global Variables---------------*/

/*
 * global var "var_table" is the table we keep track of DSL variables
 */
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
    CLUSTERED
} IndexType;



typedef enum OperatorType {
    CREATE_DB,
    CREATE_TBL,
    CREATE_COL,
    CREATE_IDX,
    SELECT,
    FETCH,
    LOAD,
    HASH_JOIN,
    RELATIONAL_INSERT,
    ADD,
    SUBTRACT,
    UPDATE,
    TUPLE,
    SHUTDOWN,
    GET_MIN,
    GET_MAX,
    GET_AVG
} OperatorType;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
    OK,
    ERROR,
} StatusCode;

//typedef enum OpenFlag {
//    CREATE = 1,
//    LOAD = 2,
//} OpenFlags;

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
    int col_count;
    column* cols;
    PosFlag* cols_pos;
    int tb_size;
    int col_length;

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
    int table_count;
    table* tables;
    PosFlag* tables_pos;
    int db_size;
} db;

/* status declares an error code and associated message
 *
 */
typedef struct status {
    StatusCode code;
    char error_message[BUF_SIZE];
} status;


typedef struct result {
    int num_tuples;
    long *payload;
    ResultType type;
} result;


typedef struct val_pos{
    int val;
    int pos;
}val_pos;


typedef struct db_operator {
    // Flag to choose operator
    OperatorType type;

    //variables to be created or re-assigned
    char* lhs_var1; //first variable (LHS)
    char* lhs_var2; //second variable (LHS)

    //variables on RHS
    result* rhs_var1;
    result* rhs_var2;
    result* rhs_var3;
    result* rhs_var4;

    //create name, load filename, relational insert string, tuple's argument
    char* string;

    //column index flag
    IndexType idx_type;

    //column used
    column* col;

    //table used (relational insert)
    table* tbl;

    //limits (select)
    int low;
    int high;

    //for update
    int value;

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


typedef struct file_node{
    char* filename;
    struct file_node* next;
}file_node;

//data structure to be passed in nested loop join thread
typedef struct join_arg{
    result* val1;
    result* pos1;
    int start1;
    int end1;
    result* val2;
    result* pos2;
    int start2; //start of val1
    int end2; //end of val1 (exclusive)
    int* res_pos1;
    int* res_pos2;
    int* res_len;
}join_arg;

//data structure to be passed in shared select
typedef struct scan_arg{
    column* col; //column to read
    int lower;
    int higher;
    int start;
    int end;
    result* res;
    result* pre_select;
}scan_arg;

typedef struct partition_arg{
    int* len; //count of length
    int** part_val; //val to write, assume pre-allocated
    int** part_pos; //pos to write, assume pre-allocated
    result* val; //val to read
    result* pos; //pos to read
    int start; //start of read
    int end; //end of read
}partition_arg;

typedef struct hashjoin_arg{
    int *val1; //val1 in partition
    int *val2; //val2 in partition
    int *pos1; //pos1 in partition
    int *pos2; //pos2 in partition
    int len1;
    int len2;
    result* res_pos1;
    result* res_pos2;
}hashjoin_arg;

//lower and upper limits
typedef struct interval{
    int lower;
    int upper;
}interval;

/*-----------End of Type Definition of Struct-------------------*/


#endif //BLUS_CS165_2015_BASE_DATA_STRUCTURE_H
