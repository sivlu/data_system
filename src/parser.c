#include "parser.h"
#include <limits.h>
#include <regex.h>
#include <string.h>
#include <ctype.h>
#include <data_structure.h>
#include <cs165_api.h>

//keep global varibale pool and count of number of variables
int VAR_COUNT = 0;
variable var_pool[NUM_VARS];



// Trims the whitespace
char* trim(char *str);
int str2int(char* token, int is_high);

void parse_find_result(char* token, result* res);
void parse_find_column(char* token, column* col);
void parse_find_table(char* token, table* tbl);


// Prototype for Helper function that executes that actual parsing after
// parse_command_string has found a matching regex.
status parse_dsl(char* str, dsl* d, db_operator* op);


/*
 * I rewrote this so we don't use regex
 * This function parse a string into a db_operator
 * NOTE: op is pre-allocated
 */
status parse_command_string(char* str, dsl** commands, db_operator* op){
    log_info("Parsing: %s", str);
    char* trim_str = trim(str);
    char query[strlen(trim_str)+1];
    strcpy(query, trim_str);


    //case 0: check if comment or empty
    if (strlen(trim_str)==0 || trim_str[0]=='-'){
        op=NULL;
        status s = {OK, ""};
        return s;
    }

    //case 1: assignment (variable name involved)
    char* e = strchr(trim_str, '=');
    if (e){
        //1. separate RHS and LHS
        char *token = strtok(query, "=");
        char lhs[strlen(token) + 1];
        strncpy(lhs,token,strlen(token));
        lhs[strlen(token)]=0;
        token = strtok(NULL, "=");
        char rhs[strlen(token)+1];
        strncpy(rhs,token,strlen(token));
        rhs[strlen(token)]=0;
        printf("LHS: %s\n", lhs);
        printf("RHS: %s\n", rhs);

        //2. parse left
        token = strtok(lhs, ",");
        op->lhs_var1 = strdup(token);
        token=strtok(NULL, ",");
        if (token) op->lhs_var2 = strdup(token);


        //3. parse right, series of comparisons happens here
        token = strtok(rhs, "(");
        char* param_delim = ",";
        char* temp = strtok(NULL, "(");
        char param[strlen(temp)];
        strcpy(param,temp);
        param[strlen(temp)-1]='\0';
        printf("op: %s\n",token);
        printf("param: %s\n",param);
        //ready to parse param
        if (strcmp(token, "select")==0){
            op->type = SELECT;
            //2 cases in select
            token = strtok(param, param_delim);
            parse_find_column(token, op->col); //TODO: implement this function
            token = strtok(NULL, param_delim);
            op->low = str2int(token, 0);
            token = strtok(NULL, param_delim);
            op->high = str2int(token, 1);
            token = strtok(NULL, param_delim);
            if (token) parse_find_result(token, op->rhs_var1); //TODO: implementation

        }else if(strcmp(token, "fetch")==0){
            op->type=FETCH;
            //1 case in fetch
            token = strtok(param, param_delim);
            parse_find_column(token, op->col);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var1);


        }else if(strcmp(token, "hashjoin")==0){
            op->type=HASH_JOIN;
            //1 case
            token = strtok(param, param_delim);
            parse_find_result(token, op->rhs_var1);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var2);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var3);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var4);


        }else if(strcmp(token, "min")==0){
            op->type = GET_MIN;
            //2 cases
            token = strtok(param, param_delim);
            parse_find_result(token, op->rhs_var1);
            token = strtok(NULL, param_delim);
            if (token) parse_find_result(token, op->rhs_var2);

        }else if(strcmp(token, "max")==0){
            op->type = GET_MAX;
            //2 cases
            token = strtok(param, param_delim);
            parse_find_result(token, op->rhs_var1);
            token = strtok(NULL, param_delim);
            if (token) parse_find_result(token, op->rhs_var2);

        }else if(strcmp(token, "avg")==0){
            op->type = GET_AVG;
            //1 case
            parse_find_result(param, op->rhs_var1);

        }else if(strcmp(token, "add")==0){
            op->type = ADD;
            //1 case
            token = strtok(param, param_delim);
            parse_find_result(token, op->rhs_var1);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var2);

        }else if(strcmp(token, "sub")==0){
            op->type = SUBTRACT;
            //1 case
            token = strtok(param, param_delim);
            parse_find_result(token, op->rhs_var1);
            token = strtok(NULL, param_delim);
            parse_find_result(token, op->rhs_var2);
        }

    }
    //case 2: no assignment (no variable name)
    else{
        char* param_delim = ",";
        char* func = strtok(query, "(");
        char* temp = strtok(NULL, "(");
        char param[strlen(temp)];
        strcpy(param,temp);
        param[strlen(temp)-1]='\0';
        printf("op: %s\n",func);
        printf("param: %s\n",param);

        if (strcmp(func, "create")==0){
            char* token = strtok(param, param_delim);

        }else if(strcmp(func, "load")==0){

        }else if(strcmp(func, "relational_insert")==0){

        }else if(strcmp(func, "update")==0){

        }else if(strcmp(func, "tuple")==0){

        }else if(strcmp(func, "shutdown")==0){

        }
    }



    // Nothing was found!
    status s;
    s.code = ERROR;
    return s;
}


// Finds a possible matching DSL command by using regular expressions.
// If it finds a match, it calls parse_command to actually process the dsl.
//status parse_command_string(char* str, dsl** commands, db_operator* op)
//{
//    log_info("Parsing: %s", str);
//
//    // Trim the string of any spaces.
//    char* trim_str = trim(str);
//
//    // Create a regular expression to parse the string
//    regex_t regex;
//    int ret;
//
//    // Track the number of matches; a string must match all
//    int n_matches = 1;
//    regmatch_t m;
//
//    for (int i = 0; i < NUM_DSL_COMMANDS; ++i) {
//        dsl* d = commands[i];
//        if (regcomp(&regex, d->c, REG_EXTENDED) != 0) {
//            log_err("Could not compile regex\n");
//        }
//
//        // Bind regular expression associated with the string
//        ret = regexec(&regex, trim_str, n_matches, &m, 0);
//
//        // If we have a match, then figure out which one it is!
//        if (ret == 0) {
//            log_info("Found Command: %d\n", i);
//            // Here, we actually strip the command as appropriately
//            // based on the DSL to get the variable names.
//            return parse_dsl(trim_str, d, op);
//        }
//    }
//
//
//    // Nothing was found!
//    status s;
//    s.code = ERROR;
//    return s;
//}

status parse_dsl(char* str, dsl* d, db_operator* op)
{
    // Use the delimiters to parse out the string
    char open_paren[2] = "(";
    char close_paren[2] = ")";
    char delimiter[2] = ",";
    // char end_line[2] = "\n";
    // char eq_sign[2] = "=";

    if (d->g == CREATE_DB) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str));
        strncpy(str_cpy, str, strlen(str));

        // This gives us everything inside the (db, <db_name>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "db", but we don't need to use it
        char* db_indicator = strtok(args, delimiter);
        (void) db_indicator;

        // This gives us <db_name>
        char* db_name = strtok(NULL, delimiter);

        log_info("create_db(%s)\n", db_name);

        // Here, we can create the DB using our parsed info!
        db* db1 = NULL;
        status s = create_db(db_name, &db1);
        if (s.code != OK) {
            // Something went wrong
        }

        // TODO(USER): You must track your variable in a variable pool now!
        // This means later on when I refer to <db_name>, I should get this
        // same db*.  You can do this in many ways, including associating
        // <db_name> -> db1

        // Free the str_cpy
        free(str_cpy);

        // No db_operator required, since no query plan
        (void) op;
        status ret;
        ret.code = OK;
        return ret;
    } else if (d->g == CREATE_TABLE) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "table"
        char* tbl_indicator = strtok(args, delimiter);
        (void) tbl_indicator;

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, delimiter);

        // This gives us <db_name>, we will need this to create the full name
        char* db_name = strtok(NULL, delimiter);

        // Generate the full name using <db_name>.<tbl_name>
        char full_name[strlen(tbl_name) + strlen(db_name)];
        strncat(full_name, db_name, strlen(db_name));
        strncat(full_name, ".", 1);
        strncat(full_name, tbl_name, strlen(tbl_name));

        // This gives us count
        char* count_str = strtok(NULL, delimiter);
        int count = 0;
        if (count_str != NULL) {
            count = atoi(count_str);
        }
        (void) count;

        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);

        // Here, we can create the table using our parsed info!
        // TODO(USER): You MUST get the original db* associated with <db_name>
        // db* db1 = NULL;

        // TODO(USER): Uncomment this section after you're able to grab the db1
        // table* tbl1 = NULL;
        // status s = create_table(db1, full_name, count, &tbl1);
        // if (s.code != OK) {
        //     // Something went wrong
        // }

        // TODO(USER): You must track your variable in a variable pool now!
        // This means later on when I refer to <full_name>, I should get this
        // same table*.  You can do this in many ways, including associating
        // <full_name> -> tbl1

        // Free the str_cpy
        free(str_cpy);

        // No db_operator required, since no query plan
        status ret;
        ret.code = OK;
        return ret;
    } else if (d->g == CREATE_COLUMN) {
        // Create a working copy, +1 for '\0'
        char* str_cpy = malloc(strlen(str) + 1);
        strncpy(str_cpy, str, strlen(str) + 1);

        // This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
        strtok(str_cpy, open_paren);
        char* args = strtok(NULL, close_paren);

        // This gives us "col"
        char* col_indicator = strtok(args, delimiter);
        (void) col_indicator;

        // This gives us <col_name>, we will need this to create the full name
        char* col_name = strtok(NULL, delimiter);

        // This gives us <tbl_name>, we will need this to create the full name
        char* tbl_name = strtok(NULL, delimiter);

        // Generate the full name using <db_name>.<tbl_name>
        char full_name[strlen(tbl_name) + strlen(col_name) + 1];
        strncat(full_name, tbl_name, strlen(tbl_name));
        strncat(full_name, ".", 1);
        strncat(full_name, col_name, strlen(col_name));

        // This gives us the "unsorted"
        char* sorting_str = strtok(NULL, delimiter);
        (void) sorting_str;

        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);

        // Here, we can create the column using our parsed info!
        // TODO(USER): You MUST get the original table* associated with <tbl_name>
        // table* table1 = NULL;

        // TODO(USER): Uncomment this section after you're able to grab the tbl1
        // column* col1 = NULL;
        // status s = create_column(tbl1, full_name, &col1);
        // if (s.code != OK) {
        //     // Something went wrong
        // }

        // TODO(USER): You must track your variable in a variable pool now!
        // This means later on when I refer to <full_name>, I should get this
        // same col*.  You can do this in many ways, including associating
        // <full_name> -> col1

        // Free the str_cpy
        free(str_cpy);

        // No db_operator required, since no query plan
        status ret;
        ret.code = OK;
        return ret;
    }

    // Should have been caught earlier...
    status fail;
    fail.code = ERROR;
    return fail;
}

// Trims the whitespace
char* trim(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = 0;
    return str;
}

//convert string to int used in limits
int str2int(char* token, int is_high){
    if (strcmp(token,"null")==0){
        return is_high? INT_MAX : INT_MIN;
    }
    return atoi(token);
}