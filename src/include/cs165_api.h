/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


/*
 * NOTES:
 *
 * 1. sync_db function is replaced by write_db_file function
 * 2. removed openflag for open_db, the default is to always
 * load/overwrite the data in the file, no matter the tables exist or not
 */

#ifndef CS165_H
#define CS165_H

#include "data_structure.h"
#include "btree.h"
#include "hashtable.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

/* OPERATOR API*/
/**
 * open_db(filename, db, flags)
 * Opens the file associated with @filename and loads its contents into @db.
 *
 * If flags | Create, then it should create a new table.
 * If flags | Load, then it should load the table with the incoming data.
 *
 * Note that the database in @filename MUST contain the same name as db->name
 * (if db != NULL). If not, then return an error.
 *
 * filename: the name associated with the DB file
 * db      : the pointer to db*
 * flags   : the flags indicating the create/load options
 * returns : a status of the operation.
 */
status open_db(const char* filename, db** database);



/**
 * create_db(db_name, db)
 * Creates a database with the given database name, and stores the pointer in db
 * Also add this db to global db_table
 *
 * db_name  : name of the database, must be unique.
 * db       : pointer to the db pointer. If *db == NULL, then create_db is
 *            responsible for allocating space for the db, else it should assume
 *            that *db points to pre-allocated space.
 * returns  : the status of the operation.
 *
 * Usage:
 *  db *database = NULL;
 *  status s = create_db("db_cs165", &database)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_db(const char* db_name, db** database);

/**
 * create_table(db, name, num_columns, table)
 * Creates a table named @name in @db with @num_columns, and stores the pointer
 * in @table.
 *
 * db          : the database in which to create the table.
 * name        : the name of the new table, must be unique in the db.
 * num_columns : the non-negative number of columns in the table.
 * table       : the pointer to the table pointer. If *table == NULL, then
 *               create_table is responsible for allocating space for a table,
 *               else it assume that *table points to pre-allocated space.
 * returns     : the status of the operation.
 *
 * Usage:
 *  // Assume you have a valid db* 'database'
 *  table* tbl = NULL;
 *  status s = create_table(database, "tbl_cs165", 4, &tbl)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_table(db* database, const char* name, int num_columns, table** tb);

/**
 * create_column(table, name, col)
 * Creates a column named @name in @table, and stores the pointer in @col.
 *
 * table   : the table in which to create the column.
 * name    : the name of the column, must be unique in the table.
 * col     : the pointer to the column pointer. If *col == NULL, then
 *           create_column is responsible for allocating space for a column*,
 *           else it should assume that *col points to pre-allocated space.
 * returns : the status of the operation.
 *
 * Usage:
 *  // Assume that you have a valid table* 'tbl';
 *  column* col;
 *  status s = create_column(tbl, 'col_cs165', &col)
 *  if (s.code != OK) {
 *      // Something went wrong
 *  }
 **/
status create_column(table *tb, const char* name, column** col);

/**
 * create_index(col, type)
 * Creates an index for @col of the given IndexType. It stores the created index
 * in col->index.
 *
 * col      : the column for which to create the index.
 * type     : the enum representing the index type to be created.
 * returns  : the status of the operation.
 **/
status create_index(column* col, IndexType type);
status create_clustered_index(table* tbl, column* col);



/**
 * drop_db(db)
 * Drops the database associated with db.  You should permanently delete
 * the db and all of its tables/columns.
 *
 * db       : the database to be dropped.
 * del_file: 1 delete file, 0 o.w.
 * rm_node: 1 remove node from global linked list, 0 o.w.
 * returns  : the status of the operation.
 **/
status drop_db(db* database, int del_file, int rm_node);
/**
 * drop_table(db, table)
 * Drops the table from the db.  You should permanently delete
 * the table and all of its columns.
 *
 * db       : the database that contains the table.
 * table    : the table to be dropped.
 * returns  : the status of the operation.
 **/
status drop_table(db* database, table* tb, int del_file);
//status drop_col(db* database, table* tb, column* col, int del_file);



/* Operations API */
status relational_insert(table* tbl, const char* line);
status insert(column *col, int data);
//status delete(column *col, int *pos);
//status col_scan(comparator* f, column* col, result** r);
//status index_scan(comparator* f, column* col, result** r);
status fetch(column* col, result* pos, result** r);
status col_select_local(column* col, int low, int high, result** r, result* pre_selected);
status btree_select_local(column *col, int low, int high, result **r, result* pre_selected);
status sorted_select_local(column* col, int low, int high, result **r, result* pre_selected);
status shared_select(column *col, result* pre_selected, interval* limits, int length, result* results[]);
void* col_select_thread(void* arg);


/* Query API */
status query_prepare(const char* query, db_operator** op);
status query_execute(db_operator* op, result** results);


/* API related to opening connection*/
status prepare_open_conn(char* data_path);
static status read_db_file(const char* db_path, char* db_name);
static status read_table_file(const char* table_path, char* table_name, db* database);
static status read_col_file(const char* col_path, char* col_name, table* tb);
static void free_list(file_node* head);
/*
 * This function list files/dirs in path
 * and put them into a linked list of file_node
 * Note that it ignores files starting with '.'
 */
static status list_files(const char* path, file_node** head, int* count);



/* API related to closing a connection*/
status prepare_close_conn(char* data_path, int remove_memory);
static status write_db_file(const char* data_path, db* database);
static status write_table_file(const char* db_path, table* tb);
static status write_col_file(const char* table_path, column* col);


/* static utility functions*/
static status remove_db_from_dbtable(db* database);
static status add_db_to_dbtable(db* database);
static status remove_disk_file(char* file_path);
void free_result(result* res);

/*closing connection */
static void free_before_closing();

/*open db related*/
static status find_all_table_cols(const char* line, column*** cols, int* count, db* database);
static int parse_and_find_count(const char* title);
static void parse_and_find_names(const char* title, char tb_names[][NAME_SIZE], char col_names[][NAME_SIZE]);
static void create_and_find_cols(db* database, char tb_names[][NAME_SIZE], char col_names[][NAME_SIZE], column** cols, int count);

/*debug functions*/
void print_db_table();
void print_system(int data);
void print_db(db* database);
void print_tbl(table* tbl, int data);
void print_result(result* res);

/*index related*/
static int compare_val_pos(const void* a, const void *b);
static int compare_int(const void* a, const void* b);
static void free_index(column_index* index);
static void create_sorted_index(val_pos* index, int* vals, int len);
static int binary_search(int* array, int len, int target);


/*join functions*/
status nested_loop_join(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2);
status hash_join(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2);
status hash_join_local(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2);
void* nested_loop_join_thread(void* args);
void* partition_thread(void* arg);
void* hash_join_thread(void* arg);
static void get_partitions(result* val, result* pos, int* par_val[NUM_PARTITION], int* par_pos[NUM_PARTITION], int count[NUM_PARTITION]);

/*aggregate and updates*/
status add(result* val1, result* val2, result** res_val);
status subtract(result* val1, result* val2, result** res_val); //assume val1 - val2
status update(column *col, result* pos, int new_val);
status min(result* val, result* pos, result** min_val, result** min_pos); //if min_pos==NULL, dont get min pos
status max(result* val, result* pos, result** max_val, result** max_pos); //if max_pos==NULL, dont get max pos
status avg(result* val, result** avg_val);

//get tuple
status tuple(result* res_arr[], int num_res, char** tuple);

#endif /* CS165_H */

