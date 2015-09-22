//
// Created by Siv Lu on 9/20/15.
//

#include "cs165_api.h"


//not finished
status open_db(const char* filename, db** database, OpenFlags flags){
    status res = {OK, NULL};

    if (flags == CREATE){


    }else if (flags == LOAD){

    }else{
        res.code = ERROR;
        res.error_message = "Flag not suppored.\n";
    }
    return res;
}

status open_db(const char* filename, db** database, OpenFlags flags){

}

status drop_db(db* database){

}

status sync_db(db* database){

}

status create_db(const char* db_name, db** database){
    status res = {OK, NULL};

    //name checking: unique db name
    //
    //missing///////
    //
    //pointer checking: database space allocation
    if (*database == NULL) {
        *database = (db*)malloc(sizeof(db));
        if (*database == NULL) {
            res.code = ERROR;
            res.error_message = "Unable to allocate space for database\n";
            return res;
        }
    }
    //create database
    (*database)->name = db_name; //assign the name
    (*database)->table_count = 0; //right now there is no table in it
    (*database)->db_size = DB_SIZE; //assign the database size
    (*database)->tables = (table*)malloc(sizeof(table) * DB_SIZE); //total num of tables is DB_SIZE
    if ((*database)->tables == NULL){
        res.code = ERROR;
        res.error_message = "Unable to allocate space in database\n";
        return res;
    }
    (*database)->tables_pos = (FlagType*)malloc(sizeof(FlagType) * DB_SIZE); //array to keep track of tables
    if ((*database)->tables_pos == NULL){
        free((*database)->tables); //free tables mem
        res.code = ERROR;
        res.error_message = "Unable to allocate space in database\n";
        return res;
    }
    //initialize the pos array in db
    FlagType* temp = (*database)->tables_pos;
    for (int i = 0; i<DB_SIZE; ++i) *(temp++) = EMPTY;
    return res;
}


//NOTE: assume db has enough space for tb
status create_table(db* database, const char* name, size_t num_columns, table** tb){
    status res = {OK, NULL};

    //unique table name checking
    for (int i = 0; i<database->db_size; i++){
        if (database->tables_pos[i] == FULL && database->tables[i].name == name){
            res.code = ERROR;
            res.error_message = "Table name already exsits in database.\n";
            return res;
        }
    }
    //db size checking
    if (database->table_count == database->db_size){
        res.code = ERROR;
        res.error_message = "Not enough space in database.\n";
        return res;
    }
    //table space allocation
    int final_pos;
    for (int i = 0; i<DB_SIZE; i++){
        FlagType* pos = database->tables_pos[i];
        table* spot = database->tables[i];
        if (*pos == EMPTY) {
            if (*tb != NULL) free(*tb);
            *tb = spot; //assign tb to this spot
            final_pos = i; //keep track of the index
            break;
        }
        pos++;
    }
    //create the table
    (*tb)->name = name; //table name
    (*tb)->col_count = 0; //no columns yet
    (*tb)->tb_size = num_columns; //set the size of the table, ie num of cols
    (*tb)->col_length = COL_SIZE; //length of columns

    (*tb)->cols_pos = (FlagType*)malloc(sizeof(FlagType)*num_columns); //total space for columns
    if ((*tb)->cols_pos == NULL){
        res.code = ERROR;
        res.error_message = "Unable to allocate space in table.\n";
        return res;
    }
    (*tb)->cols = (column*)malloc(sizeof(column)*num_columns);
    if ((*tb)->cols == NULL){
        free((*tb)->cols_pos); //free mem we allocated above for cols_pos
        res.code = ERROR;
        res.error_message = "Unable to allocate space in table.\n";
        return res;
    }
    //initialize pos array in tb
    FlagType* temp = (*tb)->cols_pos;
    for (int i= 0; i<num_columns; i++) *(temp++) = EMPTY;
    //update database, only update this when no error occurred
    database->table_count++;
    *(database->tables_pos[final_pos]) = FULL; //make flag 1 at pos

    return res;
}

//NOTE: assume tb in database, tb is not null
status drop_table(db* database, table* tb){
    status res = {OK, NULL};
    //remove table from database
    for (int i = 0; i<database->db_size; i++){
        FlagType* curr_pos = database->tables_pos[i];
        table* curr_table = database->tables[i];
        if (*curr_pos == FULL && curr_table->name == tb->name){
            *curr_pos = EMPTY;
            database->table_count --;
            break;
        }
    }
    //remove the actual table
    free(tb->cols);
    free(tb->cols_pos);

    return res;
}

status create_column(table *tb, const char* name, column** col){
    status res = {OK, NULL};
    //unique col name checking
    for (int i = 0; i<tb->tb_size; ++i){
        FlagType* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == FULL && curr_col->name == name){
            res.code = ERROR;
            res.error_message = "Column name already exists in table.\n";
            return res;
        }
    }
    //table space checking
    if (tb->col_count == tb->tb_size){
        res.code = ERROR;
        res.error_message = "Not enough space in table.\n";
        return res;
    }
    //assign space in table to col
    int final_pos;
    for (int i = 0; i<tb->tb_size; ++i){
        FlagType* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == EMPTY){
            if (*col != NULL) free(*col);
            *col = curr_col;
            final_pos = i;
            break;
        }
    }
    //create this column
    (*col)->name = name; //assign col name
    (*col)->data = (int*)malloc(sizeof(int)*tb->col_length); //malloc space for this col
    (*col)->row_count = 0; //empty col, next availble pos for data is at 0
    if ((*col)->data == NULL){
        res.code = ERROR;
        res.error_message = "Unable to allocate space in column.\n";
        return res;
    }
    //update table information
    *(tb->cols_pos[final_pos]) = FULL;
    tb->col_count++;
    return res;
}

//assume enough space in col,
//might need to change the API later to include the len of col for checking
status insert(column *col, int data){
    status res = {OK, NULL};
    *(col->data[col->row_count]) = data;
    return res;
}


status delete(column *col, int *pos){

}

status update(column *col, int *pos, int new_val){

}

status col_scan(comparator *f, column *col, result **r){

}

status create_index(column* col, IndexType type){

}

status index_scan(comparator *f, column *col, result **r){

}

/* Query API */
status query_prepare(const char* query, db_operator** op);
status query_execute(db_operator* op, result** results);
