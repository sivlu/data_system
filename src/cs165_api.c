//
// Created by Siv Lu on 9/20/15.
//

#include "cs165_api.h"

extern struct db_node *db_table;


status open_db(const char* filename, db** database, OpenFlags flags){
    //remember to update the global db linked list
    //
    //
    status res = {OK, NULL};

    if (*database == NULL){
        res.code = ERROR;
        res.error_message = "Database is null.\n";
    }else {
        if (flags == CREATE) {


        } else if (flags == LOAD) {

        } else {
            //may need to change later when having more flags
            res.code = ERROR;
            res.error_message = "Flag not suppored.\n";
        }
    }
    return res;
}

/*
 * remove db from table that keeps track of db
 */
static status remove_db_from_table(db* database){
    status res = {OK, NULL};

    db_node* prev = NULL;
    db_node* temp = db_table;
    while (temp != NULL){
        if (temp->this_db->name == database->name){
            if (temp == db_table){
                //remove head
                db_table = db_table->next;

            }else{
                prev->next = temp->next;
            }
            free(temp);
            return res;
        }
        prev = temp;
        temp = temp->next;
    }
    res.code = ERROR;
    res.error_message = "Cannot find database in database table.\n";
    return res;
}

static status rm_db_file(char* db_path){
    //dir level

}

static status rm_table_file(char* table_path){
    //dir level
}

static status rm_col_file(char* col_path){
    //file level
}


status drop_db(db* database){
    status res = {OK, NULL};
    //remove the db from global linked list of db
    res = remove_db_from_table(database);
    if (res.code == ERROR) return res;
    //free the tables in database
    for (int i = 0; i<database->db_size; i++){
        PosType curr_pos = database->tables_pos[i];
        table* curr_table = database->tables[i];
        if (*curr_pos == FULL) drop_table(database, curr_table);
    }
    //free database
    free(database->tables_pos);
    free(database->tables);
    free(database);
    //remove disk files associated with this database
    //
    //
    //

    return res;
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
            res.error_message = "Unable to allocate space for database.\n";
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
        res.error_message = "Unable to allocate space in database.\n";
        return res;
    }
    (*database)->tables_pos = (PosFlag*)malloc(sizeof(PosFlag) * DB_SIZE); //array to keep track of tables
    if ((*database)->tables_pos == NULL){
        free((*database)->tables); //free tables mem
        res.code = ERROR;
        res.error_message = "Unable to allocate space in database.\n";
        return res;
    }
    //initialize the pos array in db
    PosFlag* temp = (*database)->tables_pos;
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
        PosFlag* pos = database->tables_pos[i];
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

    (*tb)->cols_pos = (PosFlag*)malloc(sizeof(PosFlag)*num_columns); //total space for columns
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
    PosFlag* temp = (*tb)->cols_pos;
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
        PosFlag* curr_pos = database->tables_pos[i];
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
        PosFlag* curr_pos = tb->cols_pos[i];
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
        PosFlag* curr_pos = tb->cols_pos[i];
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


status col_scan(comparator *f, column *col, result **r){
    status res = {OK, NULL};
    //check if result is allocated
    if (*r == NULL) *r = (result*)malloc(sizeof(result));
    if (*r == NULL){
        res.code = ERROR;
        res.error_message = "Unable to allocate space for result.\n";
        return res;
    }
    //scan the column
    int temp_result[col->row_count]; //initialize a <vec_pos>
    int count = 0; //actual count for the result
    //
    //
    //
    //
    //
    //
    //
    //initialize result
    (*r)->payload = (int*)malloc(sizeof(int)*(count));
    (*r)->num_tuples = count;
    if ((*r)->payload == NULL){
        res.code = ERROR;
        res.error_message = "Unable to allocate space in result.\n";
        return res;
    }
    //fetch data
    status fetch_status = fetch(col, temp_result, count, r);
    return fetch_status;

}

status fetch(column* col, int* pos, int length, result** r){
    status res = {OK, NULL};
    //check if result is allocated
    if (*r == NULL) {
        *r = (result *) malloc(sizeof(result));
        if (*r == NULL) {
            res.code = ERROR;
            res.error_message = "Unable to allocate space for result.\n";
            return res;
        }
    }
    //check if result array is allocated
    if ((*r)->payload == NULL){
        (*r)->payload = (int*)malloc(sizeof(int)*length);
        (*r)->num_tuples = length;
        if ((*r)->payload == NULL){
            res.code = ERROR;
            res.error_message = "Unable to allocate space in result.\n";
            return res;
        }
    }
    //fetch the data
    for (int i = 0; i<length; ++i){
        *((*r)->payload[i]) = *(pos[i]);
    }
    return res;
}



status delete(column *col, int *pos){

}

status update(column *col, int *pos, int new_val){

}

status create_index(column* col, IndexType type){

}

status index_scan(comparator *f, column *col, result **r){

}

/* Query API */
status query_prepare(const char* query, db_operator** op);
status query_execute(db_operator* op, result** results);
