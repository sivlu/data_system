//
// Created by Siv Lu on 9/20/15.
//

#include "cs165_api.h"

extern struct db_node *db_table;

/*
 * write all db to file
 * free(drop) all db, table, col..
 */
status prepare_close_conn(){

}


static void free_list(file_node* head){
    file_node* temp = head;
    while (temp){
        file_node* next = temp->next;
        free(temp->filename);
        free(temp);
        temp = next;
    }
}

/*
 * This function list files/dirs in path
 * and put them into a linked list of file_node
 * Note that it ignores files starting with '.'
 */
static status list_files(char* path, file_node** head, int* count){
    status res = {OK, NULL};
    DIR *dp;
    struct dirent *ep;
    file_node* temp = *head;

    dp = opendir(path);
    if (dp != NULL){
        ep = readdir(dp);
        while(ep != NULL){
            if ((ep->d_name)[0]!='.'){
                //append file to file list
                if (*head == NULL){
                    *head = (file_node*)malloc(sizeof(file_node));
                    (*head)->filename = strdup(ep->d_name);
                }else{
                    temp->next = (file_node*)malloc(sizeof(file_node));
                    temp->next->filename = strdup(ep->d_name);
                    temp = temp->next;
                }
            }
            ep = readdir(dp);
        }
    }else{
        res.code = ERROR;
        res.error_message = "Cannot open directory.\n";
    }
    return res;
}

/*
 * open data folder
 * read all db from data files into memory
 * for each db, read table and cols
 */
status prepare_open_conn(char* data_path){
    status res = {OK, NULL};
    //
    //ERROR CHECKING....
    //
    //

    file_node *db_list;
    int db_num;
    //list all the db dirs
    res = list_files(data_path, &db_list, &db_num);
    if (res.code == ERROR) return res;
    //for each db, call read db file
    file_node* temp = db_list;
    while (temp != NULL){
        char file_path[BUF_SIZE];
        strcpy(file_path, data_path);
        strcat(file_path, temp->filename);
        strcat(file_path, "/"); //convention for dir
        res = read_db_file(file_path, temp->filename);
        if (res.code == ERROR) return res;
        temp = temp->next;
    }
    //free memory
    free_list(db_list);

    return res;
}

/*
 * call create db, then for each table dir, call read_table_file
 */
status read_db_file(const char* db_path, char* db_name){
    status res = {OK, NULL};

    db* database; //NOTE: check for memory errors afterwards
    file_node* table_list;
    int table_num;

    //create db in memory
    create_db(db_name, &database);
    //find all tables in this db
    res = list_files(data_path, &table_node, &table_num);
    if (res.code == ERROR) return res;
    //for each table in db, call create table
    file_node* temp = table_list;
    while (temp != NULL){
        char file_path[BUF_SIZE];
        strcpy(file_path, data_path);
        strcat(file_path, temp->filename);
        strcat(file_path, "/"); //convention for dir
        res = read_table_file(file_path, temp->filename, database);
        if (res.code == ERROR) return res;
        temp = temp->next;
    }
    //free list
    free_list(table_list);

    return res;
}

status read_table_file(const char* table_path, char* table_name, db* database){
    status res ={OK, NULL};

    table* tb;
    file_node* col_list;
    int col_num;
    //get list of cols in table and count
    res = list_files(table_path, &col_list, &col_num);
    if (res.code == ERROR) return res;
    //create table
    create_table(database, table_name, col_num, &tb);
    //for each col in table, call read col file
    file_node* temp = col_list;
    while (temp != NULL) {
        char file_path[BUF_SIZE];
        strcpy(file_path, data_path);
        strcat(file_path, temp->filename);
        res = read_col_file(file_path, temp->filename, tb);
        if (res.code == ERROR) return res;
        temp = temp->next;
    }
    //free list
    free_list(col_list);

    return res;
}

status read_col_file(const char* col_path, char* col_name, table* tb){
    status res = {OK, NULL};

    column* col;
    //create a new col in table
    create_column(tb, col_name, &col);
    //open file, get ready for reading
    FILE* f = fopen(col_path, "r");
    if (f == NULL){
        res.code = ERROR;
        res.error_message = "Error opening file to read.\n";
        return res;
    }
    //read each line in file and insert to table
    int data;
    while (fscanf(f, "%d\n", &data) != EOF){
        insert(col, data);
    }
    //close file, return result
    fclose(f);
    return res;
}


status open_db(const char* filename, db** database, OpenFlag flags){
    //remember to update the global db linked list
    //
    //
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
 * remove db from db table that keeps track of db
 */
static status remove_db_from_dbtable(db* database){
    status res = {OK, NULL};

    db_node* prev = NULL;
    db_node* temp = db_table;
    while (temp != NULL){
        if (strcmp(temp->this_db->name, database->name) == 0){
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
/*
 * execute command line to remove the dir/file of db/tb/col
 */
static status remove_disk_file(char* path){
    status res = {OK, NULL};
    //create command to rm the db dir
    char cmd[BUF_SIZE];
    strcpy(cmd, "rm -rf ");
    strcat(cmd, db_path);
    int val = system(cmd);
    if (val < 0){
        res.code = ERROR;
        res.error_message = "Error removing path.\n";
    }
    return res;
}


/*
 * write a column to disk file
 * table_path: the path of the table where the col is
 * col: pointer to the col
 */
static status write_col_file(const char* table_path, column* col){
    status res = {OK, NULL};

    //create path
    char filename[BUF_SIZE];
    strcpy(filename, table_path);
    strcat(filename, col->name);
    //open a file and write
    FILE* f  = fopen(path, "w);
    if (f == NULL){
        res.code = ERROR;
        res.error_message = "Error opening file.\n";
        return res;
    }
    //NOTE: may need to change, as some rows may not contain data
    //or we need to read data from somewhere else
    for (int i = 0; i<col->row_count; ++i) {
        int* data = col->data[i];
        fprintf(f, "%d\n", *data);
    }
    fclose(f);
    return res;
}

/*
 * write a table to disk file
 * db_path: path to db where this table is in
 * tb: pointer to table
 */
static status write_table_file(const char* db_path, table* tb){
    status res = {OK, NULL};

    //create command
    char path[BUF_SIZE];
    char cmd[BUF_SIZE];
    strcpy(path, db_path);
    strcat(path, tb->name);
    strcat(path, "/"); //convention
    strcpy(path, "mkdir ");
    strcat(path, path);

    //create table folder using cmd
    int r = system(cmd);
    if (r < 0){
        res.code = ERROR;
        res.error_message = "Error creating table folder\n";
        return res;
    }

    //for each column, create a file
    for (int i = 0; i<tb->tb_size; i++){
        PosFlag* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == FULL) {
            res = write_col_file(path, curr_col);
            if (res.code == ERROR) return res;
        }
    }
    return res;
}

/*
 * write a database to file
 * data_path: data dir that contains the db
 * database: pointer to database
 */
static status write_db_file(const char* data_path, db* database){
    status res = {OK, NULL};

    //create a command
    char path[BUF_SIZE]; //path of database dir
    char cmd[BUF_SIZE]; //command to execute
    strcpy(path, data_path);
    strcat(path, database->name);
    strcat(path, "/"); //convention: have / at the end for folder
    strcpy(cmd, "mkdir ");
    strcat(cmd, path);

    //create db folder using command line cmd
    int r = system(cmd);
    if (r < 0){
        res.code = ERROR;
        res.error_message = "Error creating database folder\n";
        return res;
    }

    //for each table in this db, create dir for it
    for (int i = 0; i<database->db_size; ++i){
        PosFlag* curr_pos = database->tables_pos[i];
        table* curr_table = database->tables[i];
        if (*curr_pos == FULL){
            res = write_table_file(path, curr_table);
            if (res.code == ERROR) return res;
        }
    }
    return res;
}


/*
 *
 */
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
    free(database->name);
    free(database);

    //remove disk file of db
    //
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
    //create database -- assign values
    (*database)->table_count = 0; //right now there is no table in it
    (*database)->db_size = DB_SIZE; //assign the database size
    //malloc space in db and check return value
    (*database)->name = strdup(db_name); //malloc and copy the name
    if ((*database)->name == NULL || (*database)->tables == NULL || (*database)->tables_pos == NULL){
        if ((*database)->name != NULL) free((*database)->name);
        if ((*database)->tables != NULL) free((*database)->tables);
        if ((*database)->tables_pos != NULL) free((*database)->tables_pos));

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
    (*tb)->col_count = 0; //no columns yet
    (*tb)->tb_size = num_columns; //set the size of the table, ie num of cols
    (*tb)->col_length = COL_SIZE; //length of columns
    //allocate space in table and check result
    (*tb)->name = strdup(name); //table name
    (*tb)->cols_pos = (PosFlag*)malloc(sizeof(PosFlag)*num_columns); //total space for columns
    (*tb)->cols = (column*)malloc(sizeof(column)*num_columns);
    if ((*tb)->cols == NULL || (*tb)->cols_pos==NULL || (*tb)->name == NULL){
        if ((*tb)->cols != NULL) free((*tb)->cols);
        if ((*tb)->name != NULL) free((*tb)->name);
        if ((*tb)->cols_pos != NULL) free((*tb)->cols_pos);
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
            database->table_count--;
            break;
        }
    }
    //delete each col
    for (int i = 0; i<tb->cols_pos; i++){
        PosFlag* curr_pos = tb->cols_pos[i];
        col* curr_col = tb->cols[i];
        if (*curr_pos == FULL){
            drop_column(tb, curr_col);
        }
    }
    //free allocated space in table
    free(tb->name);
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
        if (*curr_pos == FULL && strcmp(curr_col->name,name) == 0){
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
    (*col)->row_count = 0; //empty col, next availble pos for data is at 0
    //allocate space in column
    (*col)->name = strdup(name); //assign col name
    (*col)->data = (int*)malloc(sizeof(int)*tb->col_length); //malloc space for this col
    if ((*col)->data == NULL || (*col)->name == NULL){
        if ((*col)->data != NULL) free((*col)->data);
        if ((*col)->name != NULL) free((*col)->name);
        res.code = ERROR;
        res.error_message = "Unable to allocate space in column.\n";
        return res;
    }
    //update table information
    *(tb->cols_pos[final_pos]) = FULL;
    tb->col_count++;
    return res;
}

status drop_col(table* tb, column* col){
    status res = {OK, NULL};
    //remove col from tb
    for (int i = 0; i<tb->tb_size; i++){
        PosFlag* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == FULL && strcmp(col->name, curr_col->name)==0){
            *curr_pos == EMPTY;
            tb->col_count--;
            break;
        }
    }
    //remove actual col
    free(col->name);
    free(col->data);

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
