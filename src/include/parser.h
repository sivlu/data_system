#ifndef PARSER_H__
#define PARSER_H__

#include "cs165_api.h"
#include "dsl.h"
#include "utils.h"

#define NUM_VARS 100

typedef struct var_table{
    char* var_names[NUM_VARS]; //null at the beginning
    result* var_results[NUM_VARS];
}var_table;

void init_var_table();
void remove_var(int index);
result** get_var(char* var_name);
result** add_var(char* var_name);
result** create_var_in_pool(char* var_name);
void free_variable_pool();


char* trim(char *str);
//status parse_dsl(char* str, db_operator* op);
int str2int(char* token, int is_high);

void free_op(db_operator* op);

void parse_find_result(char* token, result** res);
void parse_find_column(char* token, column** col);
void parse_find_table(char* token, table** tbl);

// This parses the command string and then update the db_operator if it requires
// a specific query plan to be executed.
// This can automatically run jobs that don't require a query plan,
// (e.g., create_db, create_tbl, create_col);
//
// Usage: parse_command_string(input_query, commands, operator);
status parse_command_string(char* str, db_operator* op);

#endif // PARSER_H__
