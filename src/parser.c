#include "./include/parser.h"
#include <limits.h>
#include <regex.h>
#include <string.h>
#include <ctype.h>
#include "./include/data_structure.h"
#include "./include/cs165_api.h"

//keep global varibale pool and count of number of variables
var_table variable_pool;
extern struct db_node *db_table;

void free_op(db_operator* op){
    if (op->string) free(op->string);
    if (op->lhs_var1) free(op->lhs_var1);
    if (op->lhs_var2) free(op->lhs_var2);
    free(op);
}



void parse_find_result(char* token, result** res){
    for (int i= 0; i<NUM_VARS; ++i){
        char* cur_name = variable_pool.var_names[i];
        if (cur_name && strcmp(cur_name, token)==0){
            printf("Found var [%s] in var pool\n", cur_name);
            *res = (variable_pool.var_results[i]);
            printf("[%s] has len [%d] is type [%s]\n", cur_name, (*res)->num_tuples, (*res)->type==POS?"pos":"val");
        }
    }
}

void parse_find_column(char* token, column** col){
    printf("parsing [%s] to find col name\n",token);
    char temp[strlen(token)+1];
    strcpy(temp, token);
    temp[strlen(token)] = 0;
    char* tk = strtok(temp, ".");
    char* tb_name = strtok(NULL, ".");
    char* col_name = strtok(NULL, ".");


    table* tbl;
    db* cur_db = db_table->this_db; //head of list since we only use 1 in test
    for (int i=0; i<cur_db->db_size; ++i){
        if (cur_db->tables_pos[i]==FULL && strcmp(cur_db->tables[i].name, tb_name)==0){
            tbl = &(cur_db->tables[i]);
        }
    }

    for (int i=0 ;i<tbl->col_count; ++i){
        if (strcmp(tbl->cols[i].name, col_name)==0){
            *col = &(tbl->cols[i]);
            return;
        }
    }
    *col = NULL;

}
void parse_find_table(char* token, table** tbl){
    printf("parsing [%s] to find table name\n",token);

    char temp[strlen(token)+1];
    strcpy(temp, token);
    temp[strlen(token)] = 0;
    char* tk = strtok(temp, ".");
    char* tb_name = strtok(NULL, ".");

    db* cur_db = db_table->this_db; //head of list since we only use 1 in test
//    print_db(cur_db);

    for (int i=0; i<cur_db->db_size; ++i){
        if (cur_db->tables_pos[i]==FULL && strcmp(cur_db->tables[i].name, tb_name)==0){
            *tbl = &(cur_db->tables[i]);
            return;
        }
    }
    *tbl = NULL;
}

void init_var_table(){
    for (int i = 0; i<NUM_VARS; ++i){
        variable_pool.var_names[i] = NULL;
        variable_pool.var_results[i] = NULL;
    }
}


//clean everything in the pool
void free_variable_pool(){
    for (int i=0; i<NUM_VARS; ++i){
        remove_var(i);
    }
}


void remove_var(int i){
    free(variable_pool.var_names[i]);
    free_result(variable_pool.var_results[i]);
    variable_pool.var_names[i] = NULL;
    variable_pool.var_results[i] = NULL;
}

/* retunr a ptr to a result* in var pool if var exists
 * o.w. NULL
 */
result** get_result_ptr(char* var_name){
    for (int i= 0; i<NUM_VARS; ++i){
        char* cur_name = variable_pool.var_names[i];
        if (cur_name && strcmp(cur_name, var_name)==0){
            printf("found var [%s] in var pool at [%d]\n", cur_name, i);
            return &(variable_pool.var_results[i]);
        }
    }
    printf("Can't find var [%s] in var pool\n", var_name);
    return NULL;
}


//add variable name to the variable pool
//returns the pointer to the result variable
result** add_var(char* var_name){
    for (int i=0; i<NUM_VARS; ++i){
        char* cur_name = variable_pool.var_names[i];
        if (!cur_name){
            printf("adding var [%s] to pool at idx [%d]\n", var_name, i);
            variable_pool.var_names[i] = strdup(var_name);
            return &(variable_pool.var_results[i]);
        }
    }
}


/*
 * if var name already exist, overwrite by freeing the current result
 * else find a new spot
 */
//void create_var_in_pool(char* var_name, result*** res) {
//    get_var(var_name, res);
//    if (res && *res) free_result(*res);
//    else add_var(var_name, res);
//}

result** create_var_in_pool(char* var_name){
    result** temp = get_result_ptr(var_name);
    if (temp && *temp){
        free_result(*temp);
        return temp;
    }else{
        return add_var(var_name);
    }
}


/*
 * I rewrote this so we don't use regex
 * This function parse a string into a db_operator
 * NOTE: op is pre-allocated
 */
status parse_command_string(char* str, db_operator* op){
    log_info("Parsing: %s", str);
    char* trim_str = trim(str);
//    char* trim_str = str;
    char query[strlen(trim_str)+1];
    strcpy(query, trim_str);
    query[strlen(trim_str)] = 0;


    //case 0: check if comment or empty
    if (strlen(trim_str)==0 || trim_str[0]=='-'){
        op=NULL;
        status s = {OK, ""};
        return s;
    }

    //case 0.5: shutdown
    if (strcmp(trim_str, "shutdown")==0) {
        op->type = SHUTDOWN;
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
        //parse all possible arguments
        char* token1 = strtok(param, param_delim);
        char* token2 = strtok(NULL, param_delim);
        char* token3 = strtok(NULL, param_delim);
        char* token4 = strtok(NULL, param_delim);

        //ready to parse param
        if (strcmp(token, "select")==0){
            op->type = SELECT;
            //2 cases in select
            parse_find_column(token1, &(op->col));
            op->low = str2int(token2, 0);
            op->high = str2int(token3, 1);
            if (token4) parse_find_result(token4, &(op->rhs_var1));

        }else if(strcmp(token, "fetch")==0){
            op->type=FETCH;
            //1 case in fetch
            parse_find_column(token1, &(op->col));
            printf("found column: %s\n", token1);
            parse_find_result(token2, &(op->rhs_var1));
            printf("found varibale: %s\n", token2);


        }else if(strcmp(token, "hashjoin")==0){
            op->type=HASH_JOIN;
            //1 case
            parse_find_result(token1, &(op->rhs_var1));
            parse_find_result(token2, &(op->rhs_var2));
            parse_find_result(token3, &(op->rhs_var3));
            parse_find_result(token4, &(op->rhs_var4));


        }else if(strcmp(token, "min")==0){
            op->type = GET_MIN;
            //2 cases
            parse_find_result(token1, &(op->rhs_var1));
            if (token2) parse_find_result(token2, &(op->rhs_var2));

        }else if(strcmp(token, "max")==0){
            op->type = GET_MAX;
            //2 cases
            parse_find_result(token1, &(op->rhs_var1));
            if (token2) parse_find_result(token2, &(op->rhs_var2));

        }else if(strcmp(token, "avg")==0){
            op->type = GET_AVG;
            //1 case
            parse_find_result(param, &(op->rhs_var1));

        }else if(strcmp(token, "add")==0){
            op->type = ADD;
            //1 case
            parse_find_result(token1, &(op->rhs_var1));
            parse_find_result(token2, &(op->rhs_var2));

        }else if(strcmp(token, "sub")==0){
            op->type = SUBTRACT;
            //1 case
            parse_find_result(token1, &(op->rhs_var1));
            parse_find_result(token2, &(op->rhs_var2));
        }

    }
    //case 2: no assignment (no variable name)
    else{
        char* param_delim = ",";
        char* token = strtok(query, "(");
        char* temp = strtok(NULL, "(");
        char param[strlen(temp)];
        char param_reserved[strlen(temp)];
        strcpy(param,temp);
        strcpy(param_reserved,temp);
        param[strlen(temp)-1]=0;
        param_reserved[strlen(temp)-1]=0;
        printf("op: %s\n",token);
        printf("param: %s\n",param);
        //get all possible arguments first
        char* token1 = strtok(param, param_delim);
        char* token2 = strtok(NULL, param_delim);
        char* token3 = strtok(NULL, param_delim);


        if (strcmp(token, "create")==0){
            if (strcmp(token1, "db")==0){
                op->type = CREATE_DB;
                op->string = strdup(token2);
            }else if (strcmp(token1, "idx")==0){
                op->type = CREATE_IDX;
                parse_find_column(token2, &(op->col));
                parse_find_table(token2, &(op->tbl));
                if (token3[0]=='b') op->idx_type=B_PLUS_TREE;
                else if (token3[0]=='s') op->idx_type=SORTED;
                else op->idx_type=CLUSTERED;
            }


        }else if(strcmp(token, "load")==0){
            op->type = LOAD;
            op->string = strdup(param);


        }else if(strcmp(token, "relational_insert")==0){
            op->type = RELATIONAL_INSERT;
            char* tb_name = strtok(param_reserved, param_delim);
            char* longarg = strtok(NULL, "");

            parse_find_table(tb_name, &(op->tbl));
            op->string = strdup(longarg);


        }else if(strcmp(token, "update")==0){
            op->type = UPDATE;
            parse_find_column(token1, &(op->col));
            parse_find_result(token2, &(op->rhs_var1));
            op->value = atoi(token3);


        }else if(strcmp(token, "tuple")==0){
            op->type = TUPLE;
            op->string = strdup(param);

        }
    }
    printf("finished parsing!!\n");
    status s;
    s.code=OK;
    return s;


}


//int main(){
//    char* string = "create(db,\"mydb\")";
//    db_operator* op = (db_operator*)malloc(sizeof(db_operator));
//    parse_command_string(string, op);
//}

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

//status parse_dsl(char* str, dsl* d, db_operator* op)
//{
//    // Use the delimiters to parse out the string
//    char open_paren[2] = "(";
//    char close_paren[2] = ")";
//    char delimiter[2] = ",";
//    // char end_line[2] = "\n";
//    // char eq_sign[2] = "=";
//
//    if (d->g == CREATE_DB) {
//        // Create a working copy, +1 for '\0'
//        char* str_cpy = malloc(strlen(str));
//        strncpy(str_cpy, str, strlen(str));
//
//        // This gives us everything inside the (db, <db_name>)
//        strtok(str_cpy, open_paren);
//        char* args = strtok(NULL, close_paren);
//
//        // This gives us "db", but we don't need to use it
//        char* db_indicator = strtok(args, delimiter);
//        (void) db_indicator;
//
//        // This gives us <db_name>
//        char* db_name = strtok(NULL, delimiter);
//
//        log_info("create_db(%s)\n", db_name);
//
//        // Here, we can create the DB using our parsed info!
//        db* db1 = NULL;
//        status s = create_db(db_name, &db1);
//        if (s.code != OK) {
//            // Something went wrong
//        }
//
//        // TODO(USER): You must track your variable in a variable pool now!
//        // This means later on when I refer to <db_name>, I should get this
//        // same db*.  You can do this in many ways, including associating
//        // <db_name> -> db1
//
//        // Free the str_cpy
//        free(str_cpy);
//
//        // No db_operator required, since no query plan
//        (void) op;
//        status ret;
//        ret.code = OK;
//        return ret;
//    } else if (d->g == CREATE_TABLE) {
//        // Create a working copy, +1 for '\0'
//        char* str_cpy = malloc(strlen(str) + 1);
//        strncpy(str_cpy, str, strlen(str) + 1);
//
//        // This gives us everything inside the (table, <tbl_name>, <db_name>, <count>)
//        strtok(str_cpy, open_paren);
//        char* args = strtok(NULL, close_paren);
//
//        // This gives us "table"
//        char* tbl_indicator = strtok(args, delimiter);
//        (void) tbl_indicator;
//
//        // This gives us <tbl_name>, we will need this to create the full name
//        char* tbl_name = strtok(NULL, delimiter);
//
//        // This gives us <db_name>, we will need this to create the full name
//        char* db_name = strtok(NULL, delimiter);
//
//        // Generate the full name using <db_name>.<tbl_name>
//        char full_name[strlen(tbl_name) + strlen(db_name)];
//        strncat(full_name, db_name, strlen(db_name));
//        strncat(full_name, ".", 1);
//        strncat(full_name, tbl_name, strlen(tbl_name));
//
//        // This gives us count
//        char* count_str = strtok(NULL, delimiter);
//        int count = 0;
//        if (count_str != NULL) {
//            count = atoi(count_str);
//        }
//        (void) count;
//
//        log_info("create_table(%s, %s, %d)\n", full_name, db_name, count);
//
//        // Here, we can create the table using our parsed info!
//        // TODO(USER): You MUST get the original db* associated with <db_name>
//        // db* db1 = NULL;
//
//        // TODO(USER): Uncomment this section after you're able to grab the db1
//        // table* tbl1 = NULL;
//        // status s = create_table(db1, full_name, count, &tbl1);
//        // if (s.code != OK) {
//        //     // Something went wrong
//        // }
//
//        // TODO(USER): You must track your variable in a variable pool now!
//        // This means later on when I refer to <full_name>, I should get this
//        // same table*.  You can do this in many ways, including associating
//        // <full_name> -> tbl1
//
//        // Free the str_cpy
//        free(str_cpy);
//
//        // No db_operator required, since no query plan
//        status ret;
//        ret.code = OK;
//        return ret;
//    } else if (d->g == CREATE_COLUMN) {
//        // Create a working copy, +1 for '\0'
//        char* str_cpy = malloc(strlen(str) + 1);
//        strncpy(str_cpy, str, strlen(str) + 1);
//
//        // This gives us everything inside the (col, <col_name>, <tbl_name>, unsorted)
//        strtok(str_cpy, open_paren);
//        char* args = strtok(NULL, close_paren);
//
//        // This gives us "col"
//        char* col_indicator = strtok(args, delimiter);
//        (void) col_indicator;
//
//        // This gives us <col_name>, we will need this to create the full name
//        char* col_name = strtok(NULL, delimiter);
//
//        // This gives us <tbl_name>, we will need this to create the full name
//        char* tbl_name = strtok(NULL, delimiter);
//
//        // Generate the full name using <db_name>.<tbl_name>
//        char full_name[strlen(tbl_name) + strlen(col_name) + 1];
//        strncat(full_name, tbl_name, strlen(tbl_name));
//        strncat(full_name, ".", 1);
//        strncat(full_name, col_name, strlen(col_name));
//
//        // This gives us the "unsorted"
//        char* sorting_str = strtok(NULL, delimiter);
//        (void) sorting_str;
//
//        log_info("create_column(%s, %s, %s)\n", full_name, tbl_name, sorting_str);
//
//        // Here, we can create the column using our parsed info!
//        // TODO(USER): You MUST get the original table* associated with <tbl_name>
//        // table* table1 = NULL;
//
//        // TODO(USER): Uncomment this section after you're able to grab the tbl1
//        // column* col1 = NULL;
//        // status s = create_column(tbl1, full_name, &col1);
//        // if (s.code != OK) {
//        //     // Something went wrong
//        // }
//
//        // TODO(USER): You must track your variable in a variable pool now!
//        // This means later on when I refer to <full_name>, I should get this
//        // same col*.  You can do this in many ways, including associating
//        // <full_name> -> col1
//
//        // Free the str_cpy
//        free(str_cpy);
//
//        // No db_operator required, since no query plan
//        status ret;
//        ret.code = OK;
//        return ret;
//    }
//
//    // Should have been caught earlier...
//    status fail;
//    fail.code = ERROR;
//    return fail;
//}

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