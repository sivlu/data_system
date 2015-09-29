//
// Created by Siv Lu on 9/20/15.
//

#include "cs165_api.h"

extern struct db_node *db_table;



/*
 * open data folder where we store all the data
 * read all db from data files into memory
 * for each db, read table
 * for each table read all the cols
 * NOTE: associated static functions: read_db_file, read_table_file, read_col_file
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

static status read_db_file(const char* db_path, char* db_name){
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

static status read_table_file(const char* table_path, char* table_name, db* database){
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

static status read_col_file(const char* col_path, char* col_name, table* tb){
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

static void free_list(file_node* head){
    file_node* temp = head;
    while (temp){
        file_node* next = temp->next;
        free(temp->filename);
        free(temp);
        temp = next;
    }
}

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
 * clean the data folder for overwrite
 * look for db on variable "db_table"
 * write all db to file
 * free all db, table, col in memory..
 */
status prepare_close_conn(char* data_path){
    status res = {OK, NULL};

    //clean the data folder first
    char cmd[PATH_SIZE];
    strcpy(cmd, "rm -rf ");
    strcat(cmd, data_path);
    strcat(cmd, "*"); //cmd = "rm -rf ./data/*"
    int r = system(cmd);
    if (r < 0) {
        res.code = ERROR;
        res.error_message = "Error in cleaning data folder.\n";
        return res;
    }
    //for each existing db, write that db to file
    db_node* temp = db_table;
    while (temp != NULL){
        res = write_db_file(data_path, temp->this_db);
        if (res.code == ERROR) return res;
        temp = temp->next;
    }
    //destroy all allocated space for global vars
    free_before_closing();

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
    FILE* f  = fopen(path, "w");
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

static status add_db_to_dbtable(db* database){
    status res = {OK, NULL};

    //allocate space for a new db_node
    db_node* node = (db_node*)malloc(sizeof(db_node));
    if (node == NULL){
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space for db_node [%s]\n", database->name);
        return res;
    }
    node->this_db = database;

    //add this node to db_table
    if (db_table == NULL){
        //if db table is empty
        db_table = node;
    }else{
        //else append this node
        db_node* temp = db_table;
        while (temp->next != NULL) temp = temp->next;
        temp->next = node;
    }
    return res;
}

/*
 * execute command line to remove the dir/file of db/tb/col
 */
static status remove_disk_file(char* file_path){
    status res = {OK, NULL};
    //create command to rm the db dir
    char cmd[BUF_SIZE];
    strcpy(cmd, "rm -rf ");
    strcat(cmd, file_path);
    int val = system(cmd);
    if (val < 0){
        res.code = ERROR;
        res.error_message = "Error removing path.\n";
    }
    return res;
}






/*
 * remove database from memory and from the db table
 * if del_file flag is 1, delete the db disk folder
 */
status drop_db(db* database, int del_file){
    status res = {OK, NULL};
    //remove the db from global linked list of db
    res = remove_db_from_table(database);
    if (res.code == ERROR) return res;
    //free the tables in database
    for (int i = 0; i<database->db_size; i++){
        PosType curr_pos = database->tables_pos[i];
        table* curr_table = database->tables[i];
        if (*curr_pos == FULL) {
            //just delete table in memory
            //since if del_file is on
            //we can remove the whole folder later
            res = drop_table(database, curr_table, 0);
            if (res.code == ERROR) return res;
        }
    }

    //remove disk file of db if del_file == 1
    if (del_file){
        char path[PATH_SIZE];
        strcpy(path, DATA_PATH);
        strcat(path, database->name);
        res = remove_disk_file(path);
        if (res.code == ERROR) return res;
    }

    //free allocated space in database
    free(database->tables_pos);
    free(database->tables);
    free(database->name);
    free(database);

    return res;
}

//NOTE: assume tb in database, tb is not null
status drop_table(db* database, table* tb, int del_file){
    status res = {OK, NULL};
    //remove table from database
    for (int i = 0; i<database->db_size; i++){
        PosFlag* curr_pos = database->tables_pos[i];
        table* curr_table = database->tables[i];
        if (*curr_pos == FULL && strcmp(curr_table->name,tb->name)==0){
            *curr_pos = EMPTY;
            database->table_count--;
            break;
        }
    }
    //delete each col
    for (int i = 0; i<tb->tb_size; i++){
        PosFlag* curr_pos = tb->cols_pos[i];
        col* curr_col = tb->cols[i];
        if (*curr_pos == FULL){
            drop_column(database, tb, curr_col, 0);
        }
    }

    //delete table file on disk if needed
    if (del_file){
        char filepath[PATH_SIZE];
        strcpy(filepath, DATA_PATH);
        strcat(filepath, database->name);
        strcat(filepath,"/");
        strcat(filepath, tb->name);
        strcat(filepath, "/");
        res = remove_disk_file(filepath);
        if (res.code == ERROR) return res;
    }

    //free allocated space in table
    free(tb->name);
    free(tb->cols);
    free(tb->cols_pos);


    return res;
}

//NOTE: DSL may not have query to do this, but it's non-static anyways
status drop_col(db* database, table* tb, column* col, int del_file){
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

    //delete col file on disk if needed
    if (del_file){
        char filepath[PATH_SIZE];
        strcpy(filepath, DATA_PATH);
        strcat(filepath, database->name);
        strcat(filepath,"/");
        strcat(filepath, tb->name);
        strcat(filepath, "/");
        strcat(filepath, col->name);
        res = remove_disk_file(filepath);
        if (res.code == ERROR) return res;
    }

    //remove allocated space in col
    free(col->name);
    free(col->data);

    return res;
}

status create_db(const char* db_name, db** database){
    status res = {OK, NULL};

    //name checking: unique db name
    db_node* temp = db_table;
    while (temp != NULL){
        if (strcmp(temp->this_db->name, db_name) == 0){
            res.code = ERROR;
            sprintf(res.error_message, "Database name [%s] already exists.\n", db_name);
            return res;
        }
        temp = temp->next;
    }

    //pointer checking: database space allocation
    if (*database == NULL) {
        *database = (db*)malloc(sizeof(db));
        if (*database == NULL) {
            res.code = ERROR;
            sprintf(res.error_message, "Unable to allocate space for db [%s].\n", db_name);
            return res;
        }
    }

    //malloc space in db and check return value
    (*database)->name = strdup(db_name); //malloc and copy the name
    (*database)->tables = (table*)malloc(sizeof(table)*DB_SIZE);
    (*database)->tables_pos = (PosFlag*)malloc(sizeof(PosFlag)*DB_SIZE);
    if ((*database)->name == NULL || (*database)->tables == NULL || (*database)->tables_pos == NULL){
        if ((*database)->name != NULL) free((*database)->name);
        if ((*database)->tables != NULL) free((*database)->tables);
        if ((*database)->tables_pos != NULL) free((*database)->tables_pos);

        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space in db [%s].\n", db_name);
        return res;
    }

    //initialize fields in db
    (*database)->table_count = 0; //right now there is no table in it
    (*database)->db_size = DB_SIZE; //assign the database size
    PosFlag* temp = (*database)->tables_pos;
    for (int i = 0; i<DB_SIZE; ++i) *(temp++) = EMPTY;

    //add db in db_table
    res = add_db_to_dbtable(*database);
    if (res.code == ERROR) return res; //keep this if-statement, may modify later

    return res;
}


//NOTE: assume db has enough space for tb
status create_table(db* database, const char* name, size_t num_columns, table** tb){
    status res = {OK, NULL};

    //unique table name checking
    for (int i = 0; i<database->db_size; i++){
        table* curr_table = database->tables[i];
        PosFlag* curr_pos = database->tables_pos[i];
        if (*curr_pos == FULL && strcmp(curr_table->name, name) == 0){
            res.code = ERROR;
            sprintf(res.error_message, "Table [%s] already exsits in database [%s].\n", name, database->name);
            return res;
        }
    }
    //db size checking
    if (database->table_count == database->db_size){
        res.code = ERROR;
        sprintf(res.error_message, "Database [%s] is full.\n", database->name);
        return res;
    }
    //table space allocation
    int final_pos;
    for (int i = 0; i<DB_SIZE; i++){
        PosFlag* pos = database->tables_pos[i];
        table* spot = database->tables[i];
        if (*pos == EMPTY) {
            if (*tb != NULL && *tb != spot) free(*tb);
            *tb = spot; //assign tb to this spot
            final_pos = i; //keep track of the index
            break;
        }
    }

    //allocate space in table and check result
    (*tb)->name = strdup(name); //table name
    (*tb)->cols_pos = (PosFlag*)malloc(sizeof(PosFlag)*num_columns); //total space for columns
    (*tb)->cols = (column*)malloc(sizeof(column)*num_columns);
    if ((*tb)->cols == NULL || (*tb)->cols_pos==NULL || (*tb)->name == NULL){
        if ((*tb)->cols != NULL) free((*tb)->cols);
        if ((*tb)->name != NULL) free((*tb)->name);
        if ((*tb)->cols_pos != NULL) free((*tb)->cols_pos);
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space in table [%s].\n", name);
        return res;
    }
    //initialize fields in tb
    (*tb)->col_count = 0; //no columns yet
    (*tb)->tb_size = num_columns; //set the size of the table, ie num of cols
    (*tb)->col_length = COL_SIZE; //length of columns
    PosFlag* temp = (*tb)->cols_pos;
    for (int i= 0; i<num_columns; i++) *(temp++) = EMPTY;

    //update database at end cuz we want only update this when no error occurred
    database->table_count++;
    *(database->tables_pos[final_pos]) = FULL; //make flag 1 at pos

    return res;
}


status create_column(table* tb, const char* name, column** col){
    status res = {OK, NULL};

    //unique col name checking
    for (int i = 0; i<tb->tb_size; ++i){
        PosFlag* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == FULL && strcmp(curr_col->name, name) == 0){
            res.code = ERROR;
            sprintf(res.error_message, "Column [%s] already exists in table [%s].\n", name, tb->name);
            return res;
        }
    }

    //table space checking
    if (tb->col_count == tb->tb_size){
        res.code = ERROR;
        sprintf(res.error_message, "Table [%s] is full.\n", tb->name);
        return res;
    }

    //assign space in table to col
    int final_pos;
    for (int i = 0; i<tb->tb_size; ++i){
        PosFlag* curr_pos = tb->cols_pos[i];
        column* curr_col = tb->cols[i];
        if (*curr_pos == EMPTY){
            if (*col != NULL && *col != curr_col) free(*col);
            *col = curr_col;
            final_pos = i;
            break;
        }
    }

    //allocate space in column
    (*col)->name = strdup(name); //malloc and assign col name
    (*col)->data = (int*)malloc(sizeof(int)*tb->col_length); //malloc space for this col
    (*col)->index = (column_index*)malloc(sizeof(column_index)*tb->col_length);//malloc for index
    if ((*col)->data == NULL || (*col)->name == NULL || (*col)->index == NULL){
        if ((*col)->data != NULL) free((*col)->data);
        if ((*col)->name != NULL) free((*col)->name);
        if ((*col)->index != NULL) free((*col)->index);
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space in column [%s].\n", name);
        return res;
    }
    //initialize fields in col
    (*col)->row_count = 0; //empty col, next available pos for data is at 0
    //update table information
    *(tb->cols_pos[final_pos]) = FULL;
    tb->col_count++;
    return res;
}






// ---------start of not finished..------------------


//NOTE: assume file in correct format
status open_db(const char* filename, db** database){
    status res = {OK, NULL};

    if (*database == NULL){
        res.code = ERROR;
        sprintf(res.error_message,"Database [%s] does not exist.\n", (*database)->name);
    }else {
        tb_col* headers;
        res = find_all_table_cols(filename, &headers, *database);
        //
        //
        //
        free(headers);
    }
    return res;
}



static status find_all_table_cols(const char* filename, tb_col** headers, db* database){
    status res = {OK, NULL};

    //read first line of file and parse into tb and col names
    char *tb_names, *col_names;
    int count = 0;
    int len = 0;
    FILE* f = fopen(filename, "r");
    char* title = NULL;
    if (getline(&title, &len, f) == -1){
        res.code = ERROR;
        sprintf(res.error_message, "Unable to read file [%s].\n", filename);
        return res;
    }
    parse_and_find_names(title, &tb_names, &col_names, &count);
    fclose(f);
    free(title);
    //create create tables and cols as needed

    //construct headers
    *headers = (tb_col*)malloc(sizeof(tb_col)*count);
    for (int i = 0; i<count; ++i){


    }



}

static void parse_and_find_names(char* title, char** tb_names, char** col_names, int* count){

}

static void* contains(void* container, char* name, int db_contains_tb){
    //if container contains name, returns pointer to that item
    //else return NULL
    void* temp;
    PosFlag* pos;
    int len;
    if (db_contains_tb){
        //db tb relation
        container = (db*)container;
        temp = (table*)container->tables;
        pos = container->tables_pos;
        len = container->db_size;
    }else{
        //tb col relation
        container = (table*)container;
        temp = (col*)container->cols;
        pos = container->cols_pos;
        len = container->tb_size;
    }
    for (int i = 0; i<len; ++i){
        if (*(pos[i]) == FULL && strcmp((temp[i])->name, name) == 0) return temp;
    }
    return NULL;
}




//assume enough space in col,
//might need to change the API later to include the len of col for checking
status insert(column *col, int data){
    status res = {OK, NULL};
    *(col->data[col->row_count]) = data;
    col->row_count++;
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
