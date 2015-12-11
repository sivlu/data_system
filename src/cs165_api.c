//
// Created by Siv Lu on 9/20/15.
//

#include "./include/cs165_api.h"
struct db_node *db_table;


/*
 * open data folder where we store all the data
 * read all db from data files into memory
 * for each db, read table
 * for each table read all the cols
 * NOTE: associated static functions: read_db_file, read_table_file, read_col_file
 */
status prepare_open_conn(char* data_path){
    status res = {OK, ""};
    //
    //ERROR CHECKING....
    //
    //

    file_node *db_list = NULL;
    long db_num = 0;
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
#ifdef DEBUG
    log_info("reading db path: %s\n", db_path);
#endif

    status res = {OK, ""};

    db* database = NULL; //NOTE: check for memory errors afterwards
    file_node* table_list = NULL;
    long table_num = 0;

    //create db in memory
    create_db(db_name, &database);


    //find all tables in this db
    res = list_files(db_path, &table_list, &table_num);
    if (res.code == ERROR) return res;

    //for each table in db, call create table
    file_node* temp = table_list;
    while (temp != NULL){
        char file_path[BUF_SIZE];
        strcpy(file_path, db_path);
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
#ifdef DEBUG
    log_info("reading tbl path: %s\n", table_path);
#endif
    status res = {OK, ""};
    //initialize
    table* tb = NULL;
    file_node* col_list = NULL;
    long col_num = 0;

    //get list of cols in table and count
    res = list_files(table_path, &col_list, &col_num);
    if (res.code == ERROR) return res;

    //create table
    res = create_table(database, table_name, col_num, &tb);
    if (res.code == ERROR) return res;

    //for each col in table, call read col file
    file_node* temp = col_list;
    while (temp != NULL) {
        char file_path[BUF_SIZE];
        strcpy(file_path, table_path);
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
#ifdef DEBUG
    log_info("reading col path: %s\n", col_path);
#endif
    status res = {OK, ""};

    column* col = NULL;
    //create a new col in table
    create_column(tb, col_name, &col);
    //open file, get ready for reading

    FILE* f = fopen(col_path, "r");
    if (f == NULL){
        res.code = ERROR;
        sprintf(res.error_message, "Error opening path [%s] to read.\n", col_path);
        return res;
    }
    //read each line in file and insert to table
    long data;
    while (fscanf(f, "%ld\n", &data) != EOF){
        insert(col, data);
    }
    //close file, return result
    fclose(f);
    return res;
}

static void free_list(file_node* head){
    if (!head) return;

    file_node* temp = head;
    while (temp){
        printf("at %s\n", temp->filename);
        file_node* tofree = temp;
        temp = temp->next;
        free(tofree->filename);
        free(tofree);
    }
}

static status list_files(const char* path, file_node** head, long* count){
#ifdef DEBUG
    log_info("listing files for path: %s\n", path);
#endif
    status res = {OK, ""};
    DIR *dp = NULL;
    struct dirent *ep = NULL;
    file_node* temp = *head;
    long c = 0;

    dp = opendir(path);
    if (dp != NULL){
        ep = readdir(dp);
        while(ep != NULL){
            if ((ep->d_name)[0]!='.'){
#ifdef DEBUG
                log_info("found file/dir: %s\n", ep->d_name);
                printf("found file/dir: %s\n", ep->d_name);
#endif
                //append file to file list
                c++;
                if (*head == NULL){
                    *head = (file_node*)malloc(sizeof(file_node));
                    (*head)->filename = strdup(ep->d_name);
                    (*head)->next = NULL;
                    temp = *head;
                }else{
                    temp->next = (file_node*)malloc(sizeof(file_node));
                    temp->next->filename = strdup(ep->d_name);
                    temp->next->next = NULL;
                    temp = temp->next;
                }
            }
            ep = readdir(dp);
        }
        *count = c;
    }else{
        res.code = ERROR;
        sprintf(res.error_message, "Cannot open directory.\n");
        printf("%s", res.error_message);
    }
    closedir(dp);
#ifdef DEBUG
    log_info("total files found: %ld\n", *count);
#endif
    return res;
}



/*
 * clean the data folder for overwrite
 * look for db on variable "db_table"
 * write all db to file
 * remove all db from memory
 * free all db, table, col in memory..
 */
status prepare_close_conn(char* data_path, long remove_memory){
    status res = {OK, ""};

    //clean the data folder first
    char cmd[PATH_SIZE];
    strcpy(cmd, "rm -rf ");
    strcat(cmd, data_path);
    strcat(cmd, "*"); //cmd = "rm -rf ./data/*"
#ifdef DEBUG
    printf("running cmd: %s\n", cmd);
#endif
    long r = system(cmd);
    if (r < 0) {
        res.code = ERROR;
        sprintf(res.error_message, "Error in cleaning data folder.\n");
        return res;
    }


    //for each existing db, write that db to file
    db_node* temp = db_table;
    while (temp != NULL){
        res = write_db_file(data_path, temp->this_db);
        if (res.code == ERROR) return res;
        temp = temp->next;
    }
    //remove db table nodes memory
    if (remove_memory) free_before_closing();

    return res;
}

/*
 * free global vars (e.g. linked list)
 * free db and tables
 */
static void free_before_closing() {
    db_node *temp = db_table;
    while (temp) {
        drop_db(temp->this_db, 0, 0);
        db_node* t= temp;
        temp = temp->next;
        free(t);
    }
    db_table = NULL;
}


/*
 * write a database to file
 * data_path: data dir that contains the db
 * database: pointer to database
 */
static status write_db_file(const char* data_path, db* database){
    status res = {OK, ""};

    //create a command
    char path[BUF_SIZE]; //path of database dir
    char cmd[BUF_SIZE]; //command to execute
    strcpy(path, data_path);
    strcat(path, database->name);
    strcat(path, "/"); //convention: have / at the end for folder
    strcpy(cmd, "mkdir ");
    strcat(cmd, path);
#ifdef DEBUG
    printf("running cmd: %s\n", cmd);
#endif
    //create db folder using command line cmd
    long r = system(cmd);
    if (r < 0){
        res.code = ERROR;
        sprintf(res.error_message,"Error creating database folder\n");
        printf("%s", res.error_message);
        return res;
    }

    //for each table in this db, create dir for it
    for (long i = 0; i<database->db_size; ++i){
        PosFlag curr_pos = database->tables_pos[i];
        table* curr_table = &(database->tables[i]);
        if (curr_pos == FULL){
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
    status res = {OK, ""};

    //create command
    char path[BUF_SIZE];
    char cmd[BUF_SIZE];
    strcpy(path, db_path);
    strcat(path, tb->name);
    strcat(path, "/"); //convention
    strcpy(cmd, "mkdir ");
    strcat(cmd, path);
#ifdef DEBUG
    printf("running cmd: %s\n", cmd);
#endif
    //create table folder using cmd
    long r = system(cmd);
    if (r < 0){
        res.code = ERROR;
        sprintf(res.error_message, "Error creating table folder\n");
        return res;
    }

    //for each column, create a file
    for (long i = 0; i<tb->tb_size; i++){
        PosFlag curr_pos = tb->cols_pos[i];
        column* curr_col = &(tb->cols[i]);
        if (curr_pos == FULL) {
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
    status res = {OK, ""};

    //create path
    char filename[BUF_SIZE];
    strcpy(filename, table_path);
    strcat(filename, col->name);
#ifdef DEBUG
    printf("writing file: %s\n", filename);
#endif
    //open a file and write
    FILE* f  = fopen(filename, "w");
    if (f == NULL){
        res.code = ERROR;
        sprintf(res.error_message, "Error opening file [%s].\n", filename);
        return res;
    }
    //NOTE: may need to change, as some rows may not contain data
    //or we need to read data from somewhere else
    for (long i = 0; i<col->row_count; ++i) {
        long val = col->data[i];
        fprintf(f, "%ld\n", val);
    }
    fclose(f);
    return res;
}



/*
 * remove db from db table that keeps track of db
 */
static status remove_db_from_dbtable(db* database){
    status res = {OK, ""};

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
    sprintf(res.error_message,"Cannot find [%s] in db table.\n", database->name);
    return res;
}

static status add_db_to_dbtable(db* database){
    status res = {OK, ""};

    //allocate space for a new db_node
    db_node* node = (db_node*)malloc(sizeof(db_node));
    if (node == NULL){
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space for db_node [%s]\n", database->name);
        return res;
    }
    node->this_db = database;
    node->next = NULL;

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
    status res = {OK, ""};
    //create command to rm the db dir
    char cmd[BUF_SIZE];
    strcpy(cmd, "rm -rf ");
    strcat(cmd, file_path);
    long val = system(cmd);
    if (val < 0){
        res.code = ERROR;
        sprintf(res.error_message ,"Error removing path [%s].\n", file_path);
    }
    return res;
}


/*
 * remove database from memory and from the db table
 * if del_file flag is 1, delete the db disk folder
 */
status drop_db(db* database, long del_file, long rm_node){
#ifdef DEBUG
//    log_info("dropping db: %s", database->name);
    printf("dropping db: %s\n", database->name);
#endif
    status res = {OK, ""};


    //free the tables in database
    for (long i = 0; i<database->db_size; i++){
        PosFlag curr_pos = database->tables_pos[i];
        table* curr_table = &(database->tables[i]);
        if (curr_pos == FULL) {
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

    //remove the db from global linked list of db
    if (rm_node) {
        res = remove_db_from_dbtable(database);
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
status drop_table(db* database, table* tb, long del_file){
#ifdef DEBUG
    log_info("dropping table: %s", tb->name);
#endif
    status res = {OK, ""};
    //remove table from database
    for (long i = 0; i<database->db_size; i++){
        PosFlag* curr_pos = &(database->tables_pos[i]);
        table* curr_table = &(database->tables[i]);
        if (*curr_pos == FULL && strcmp(curr_table->name,tb->name)==0){
#ifdef DEBUG
            log_info("removing [%s] from db", tb->name);
#endif
            *curr_pos = EMPTY;
            database->table_count--;
            break;
        }
    }

    //delete all cols. Since it relational db, we never delete individual cols
    for (long i = 0; i<tb->tb_size; i++){
        PosFlag* curr_pos = &(tb->cols_pos[i]);
        column* curr_col = &(tb->cols[i]);
        if (*curr_pos == FULL){
            *curr_pos = EMPTY;
            if (curr_col->name) free(curr_col->name);
            if (curr_col->data) free(curr_col->data);
            if (curr_col->index) {
                free_index(curr_col->index);
                free(curr_col->index);
            }
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


status create_db(const char* db_name, db** database){
    printf("Creating db [%s]\n", db_name);
    status res = {OK, ""};

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

    //malloc space in db
    (*database)->name = strdup(db_name); //malloc and copy the name
    (*database)->tables = (table*)malloc(sizeof(table)*DB_SIZE);
    (*database)->tables_pos = (PosFlag*)malloc(sizeof(PosFlag)*DB_SIZE);


    //initialize fields in db
    (*database)->table_count = 0; //right now there is no table in it
    (*database)->db_size = DB_SIZE; //assign the database size
    PosFlag* temp_pos = &((*database)->tables_pos[0]);
    for (long i = 0; i<DB_SIZE; ++i) temp_pos[i] = EMPTY;

    //add db in db_table
    add_db_to_dbtable(*database);

    return res;
}


//NOTE: assume db has enough space for tb
status create_table(db* database, const char* name, long num_columns, table** tb){
    printf("Try to create table [%s] in db [%s]\n", name, database->name);
    status res = {OK, ""};

    //unique table name checking
    for (long i = 0; i<database->db_size; i++){
        table curr_table = (database->tables[i]);
        PosFlag curr_pos = (database->tables_pos[i]);
        if (curr_pos == FULL && strcmp(curr_table.name, name) == 0){
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
    long final_pos = 0;
    for (long i = 0; i<DB_SIZE; i++){
        PosFlag pos = database->tables_pos[i];
        table* spot = &(database->tables[i]);
        if (pos == EMPTY) {
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
    PosFlag* temp = &((*tb)->cols_pos[0]);
    for (long i= 0; i<num_columns; i++) temp[i] = EMPTY;

    //update database at end cuz we want only update this when no error occurred
    database->table_count++;
    database->tables_pos[final_pos] = FULL; //make flag 1 at pos

    return res;
}


status create_column(table* tb, const char* name, column** col){
    status res = {OK, ""};

    //unique col name checking
    for (long i = 0; i<tb->tb_size; ++i){
        PosFlag curr_pos = tb->cols_pos[i];
        column curr_col = tb->cols[i];
        if (curr_pos == FULL && strcmp(curr_col.name, name) == 0){
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
    long final_pos = 0;
    for (long i = 0; i<tb->tb_size; ++i){
        PosFlag curr_pos = tb->cols_pos[i];
        column* curr_col = &(tb->cols[i]);
        if (curr_pos == EMPTY){
            if (*col != NULL && *col != curr_col) free(*col);
            *col = curr_col;
            final_pos = i;
            break;
        }
    }

    //allocate space in column
    (*col)->name = strdup(name); //malloc and assign col name
    (*col)->data = (long*)malloc(sizeof(long)*(tb->col_length)); //malloc space for this col
    if ((*col)->data == NULL || (*col)->name == NULL){
        if ((*col)->data != NULL) free((*col)->data);
        if ((*col)->name != NULL) free((*col)->name);
        if ((*col)->index != NULL) free((*col)->index);
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space in column [%s].\n", name);
        return res;
    }
    //initialize fields in col
    (*col)->row_count = 0; //empty col, next available pos for data is at 0
    (*col)->index = NULL;
    //update table information
    tb->cols_pos[final_pos] = FULL;
    tb->col_count++;
    return res;
}


//assume enough space in col,
//might need to change the API later to include the len of col for checking
status insert(column *col, long data){
    status res = {OK, ""};
    col->data[col->row_count] = data;
    col->row_count++;
    return res;
}

status relational_insert(table* tbl, const char* line){
    char copy[BUF_SIZE];
    strcpy(copy, line);
    copy[strlen(line)] =0;
    printf("relational inserting: %s\n", copy);

    char* delim = ",";
    char* token = strtok(copy, delim);
    long i = 0;
    while (token){
        insert(&(tbl->cols[i++]), atoi(token));
        token = strtok(NULL, delim);
    }
    tbl->col_length++;
}






//NOTE: assume file in correct format
status open_db(const char* filename, db** database){
#ifdef DEBUG
    log_info("reading file: %s\n", filename);
    log_info("using db: %s\n", (*database)->name);
#endif

    status res = {OK, ""};

    if (*database == NULL){
        res.code = ERROR;
        sprintf(res.error_message,"Database is null.\n");
    }else {
        column** cols = NULL; //array of column pointers
        long count = 0;
        size_t len = 0;
        char* line = NULL;
        FILE* f = fopen(filename, "r");
        if (f == NULL){
            res.code = ERROR;
            sprintf(res.error_message, "Unable to open file [%s].\n", filename);
            return res;
        }
        if (getline(&line, &len, f) == -1){
            res.code = ERROR;
            sprintf(res.error_message, "Unable to read file [%s].\n", filename);
            return res;
        }

        //create all tables, get all the cols and count of cols
        res = find_all_table_cols(line, &cols, &count, *database);
        if (res.code == ERROR) return res;

        //continue to read each line of data, parse and insert
        while (getline(&line, &len, f) != -1) {
            char* copy = strdup(line);
            char* delim = ",\n";
            char* token = strtok(copy, delim);
            long i = 0;
            while(token){
                insert((cols[i++]), atoi(token));
                token = strtok(NULL, delim);
            }
            free(copy);
        }
        //clean up
        fclose(f);
        free(line);
        free(cols);
    }
    return res;
}


/*
 * parse header line into list of table names and list of col names
 * create each table and cols in each table
 * modify headers to make it contains pointers to tables and cols
 */
static status find_all_table_cols(const char* title, column*** cols, long* count, db* database){
    status res = {OK, ""};

    //find number of cols
    *count = parse_and_find_count(title);
    //find names of tables, cols
    char tb_name[*count][NAME_SIZE], col_name[*count][NAME_SIZE];
    parse_and_find_names(title, tb_name, col_name);
    //allocate space for cols pointers
    *cols = (column**)malloc(sizeof(column*)*(*count));
    if (*cols == NULL){
        res.code = ERROR;
        sprintf(res.error_message, "Unable to allocate space for cols.\n");
        return res;
    }

    //create table and columns, assign cols pointer to columns
    create_and_find_cols(database, tb_name, col_name, *cols, *count);

    return res;
}

static long parse_and_find_count(const char* title){
#ifdef DEBUG
    printf("parsing line: %s", title);
#endif
    char* copy = strdup(title);
    long count = 0;
    char* token = strtok(copy, ",\n");
    while (token){
        count++;
        token = strtok(NULL, ",");
    }
    free(copy);
#ifdef DEBUG
    printf("total cols: %ld\n", count);
#endif
    return count;
}

static void parse_and_find_names(const char* title, char tb_names[][NAME_SIZE], char col_names[][NAME_SIZE]){
    char* copy = strdup(title);
    char* delim = ",.\n";
    char* token = strtok(copy, delim);
    long i = 0;
    long j = 0;
    while (token){
        if (j%3 == 1) {
            strcpy(tb_names[i], token); //copy tb name
        }
        if (j%3 == 2) {
            strcpy(col_names[i], token); //col name
#ifdef DEBUG
            printf("tb: %s | col: %s \n", tb_names[i], col_names[i]);
#endif
            i++;
        }
        j++;
        token = strtok(NULL, delim);
    }
    free(copy);
}


//assume all same table names are grouped together
//e.g. it's always like [tb1, tb1, tb1, tb2, tb2, ...]
static void create_and_find_cols(db* database, char tb_names[][NAME_SIZE], char col_names[][NAME_SIZE], column** cols, long count) {
    long i = 0;
    while (i < count) {
        char *cur_tb = tb_names[i];
        printf("cur tb name: %s\n", cur_tb);
        long j = i;
        while (j < count && strcmp(tb_names[j], cur_tb) == 0) {
            ++j;
        }
        long num_cols = j - i;
        printf("cur col number: %ld\n", num_cols);
        table *tb = NULL;
        create_table(database, cur_tb, num_cols, &tb);
        printf("finished creating table\n");
        for (long k = 0; k < num_cols; ++k) {
            column *cur_col = NULL;
            create_column(tb, col_names[i + k], &cur_col);
            cols[i + k] = cur_col;
        }
        i = j;
    }

}



//not using this for now, will use new signature
/*
 * Takes linked list of comparator and a col
 * return a result that contains all valid positions
 */
//status col_scan(comparator *f, column *col, result **r){







status fetch(column* col, result* pos_res, result** r){
    status res = {OK, ""};
    long length = pos_res->num_tuples;
    long* pos = pos_res->payload;


    *r = (result *) malloc(sizeof(result));
    (*r)->payload = (long*)malloc(sizeof(long)*length);
    (*r)->num_tuples = length;
    (*r)->type = VAL;


    //fetch the data
    for (long i = 0; i<length; ++i){
        (*r)->payload[i] = col->data[pos[i]];
    }
    return res;
}


status create_index(column* col, IndexType type){
    //free old index if any
    if (col->index) free_index(col->index);
    col->index = (column_index*)malloc(sizeof(column_index));
    col->index->type = type;

    if (type == SORTED){
        col->index->index = (val_pos*)malloc(sizeof(val_pos)*col->row_count);
        create_sorted_index(col->index->index, col->data, col->row_count);
    }else{
        btree_node* my_btree = NULL;
        for (long i = 0; i<col->row_count; ++i){
            insert_btree(&my_btree, col->data[i], i);
        }
        col->index->index = my_btree;
    }

//    //check result !!! turn off later
//    printf("Printing index\n");
//    if (type == SORTED) {
//        for (long i = 0; i < col->row_count; ++i) {
//            printf("(%ld,%ld) ", ((val_pos*) col->index->index)[i].val,
//                   ((val_pos*) col->index->index)[i].pos);
//        }
//    }else{
//        print_leaf_level((btree_node*)col->index->index);
//    }
}

static void create_sorted_index(val_pos* index, long* vals, long len){
    for (long i = 0; i<len; ++i){
        index[i].val = vals[i];
        index[i].pos = i;
    }
    qsort(index, len, sizeof(val_pos), compare_val_pos);
}

static void free_index(column_index* index){
    if (index->type == SORTED){
        free(index->index);
    }else{
        destroy_btree((btree_node*)index->index);
    }
}

static int compare_int(const void* a, const void* b){
    return (int)(*(long*)a - *(long*)b);
}

status create_clustered_index(table* tbl, column* col){
    long rows = col->row_count;

    //initialize val/pos pairs
    val_pos* pairs = (val_pos*)malloc(sizeof(val_pos)*rows);
    if (!pairs) printf("Malloc failed\n");


    for (long i=0; i<rows; ++i){
        pairs[i].val = col->data[i];
        pairs[i].pos = i;
    }

    //qsort pos based on col values
    qsort(pairs, rows, sizeof(val_pos), compare_val_pos);

    //sort each col based on pos
    for (long i = 0; i<tbl->tb_size; ++i){
        column* cur_col = &(tbl->cols[i]);
        long* new_data = (long*)malloc(sizeof(long)*tbl->col_length);
        long* old_data = cur_col->data;


        for (long j = 0; j<rows; ++j){
            new_data[j] = old_data[pairs[j].pos];
        }
        cur_col->data = new_data;
        free(old_data);
    }
    free(pairs);
}

static int compare_val_pos(const void* a, const void *b){
    val_pos *p1 = (val_pos*)a, *p2 = (val_pos*)b;
    if (p1->val < p2->val) return -1;
    if (p1->val == p2->val) return 0;
    return 1;

}

/*
 * success: return index
 * fail: return -1
 */
static long binary_search(long* array, long len, long target){
    long s = 0, t = len;
    long m = (s+t)/2;
    while (s < t){
        if (array[m] < target)
            s = m+1;
        else if (array[m] > target)
            t = m;
        else
            return m;
        m = (s+t)/2;
    }
    return -1;
}


/*
 * input:
 *  col: col to scan
 *  low: low limit, inclusive
 *  high: high limit, exclusive
 *  r: result, contains pos vec
 *  pre_selected: pre_selected pos vec
 *  NOTE: preselected is sorted, the returned positions are also sorted
 */
status col_select_local(column* col, long low, long high, result** r, result* pre_selected){
    status res = {OK, ""};
    //check if result is allocated
    (*r) = (result*)malloc(sizeof(result));



    //initialize local variables
    long length = pre_selected==NULL? col->row_count:pre_selected->num_tuples;
    long temp_result[length]; //initialize a <vec_pos>
    long count = 0; //actual count for the result
    //scan column
    for (long i = 0; i<length; ++i){
        long pos = pre_selected == NULL? i: pre_selected->payload[i];
        long cur_val = col->data[pos];
        if (cur_val >= low && cur_val < high) {
            temp_result[count++] = pos; //TODO:change back
//            temp_result[count++] = cur_val;
        }
    }
    //set up result
    (*r)->type = POS;
    (*r)->num_tuples = count;
    (*r)->payload = (long*)malloc(sizeof(long)*count); //didnt check allocation error
    memcpy((*r)->payload, temp_result, count*sizeof(long));

    return res;
}
/*
 * This func uses sorted index to select
 */
status sorted_select_local(column* col, long low, long high, result **r, result* pre_selected){
    status res = {OK, ""};
    //init result if not init
    (*r) = (result*)malloc(sizeof(result));


    long *temp_result = NULL;
    if (pre_selected) temp_result = (long*)malloc(sizeof(long)*pre_selected->num_tuples);
    else temp_result = (long*)malloc(sizeof(long)*col->row_count);


    val_pos* sorted_index = (val_pos*)col->index->index;
    long length = col->row_count;
    long s = 0, t= length;
    long m = (s+t)/2;
    if (sorted_index[0].val >= low){
        m = 0;
    }else {
        while (s < t) {
            if (sorted_index[m].val < low) {
                s = m + 1;
            } else if (sorted_index[m].val > low && m > 0 && sorted_index[m - 1].val < low) {
                //m is the start point
                break;
            } else if (sorted_index[m].val > low) {
                t = m;
            } else {
                //found the value
                long temp = m;
                while (temp > 0 && sorted_index[temp].val == low) temp--;
                m = temp == m ? temp : temp + 1;
                break;
            }
            m = (s + t) / 2;
        }
    }

    long count=0;
    while (sorted_index[m].val < high){
        if (pre_selected){
            if (binary_search(pre_selected->payload, pre_selected->num_tuples, sorted_index[m].pos) > -1){
                temp_result[count++] = sorted_index[m++].pos;
            }
        }else {
            temp_result[count++] = sorted_index[m++].pos;
        }
    }
    //paste over result
    (*r)->type = POS;
    (*r)->num_tuples = count;
    (*r)->payload = (long*)malloc(sizeof(long)*count); //didnt check allocation error
    memcpy((*r)->payload, temp_result, count*sizeof(long));
    //sort positions
    qsort((*r)->payload, count, sizeof(long), compare_int);
}


//assume pre selected is sorted
status btree_select_local(column *col, long low, long high, result **r, result* pre_selected){
    status res = {OK, ""};
    //init result if not init
    (*r) = (result*)malloc(sizeof(result));


    //search in btree
    long size = pre_selected==NULL? col->row_count : pre_selected->num_tuples;
    long temp_result[size];
    long count = 0;
    btree_node* start_node = search_btree((btree_node*)col->index->index, low);

    print_leaf_node(start_node);
    while (start_node){
        for (int i = 0; i<start_node->num; ++i){
            long curr = start_node->val[i];
            pos_node* pos_head = (pos_node*)start_node->ptr[i];
            if (curr >= high) break;
            if (curr >= low) {
                if (pre_selected==NULL) {
                    while (pos_head) {
                        //if nothing is preselect, we just append this to result
                        temp_result[count++] = pos_head->pos; //TODO:change back
//                        temp_result[count++] = curr;

                        pos_head = pos_head->next;
                    }
                }else{
                    while (pos_head) {
                        if (binary_search(pre_selected->payload, pre_selected->num_tuples, pos_head->pos) > -1) {
                            //if preselected, search whether this is in preselected
                            temp_result[count++] = pos_head->pos;
                        }
                        pos_head = pos_head->next;
                    }
                }
            }
        }
        start_node = (btree_node*)start_node->ptr[start_node->node_size];
    }
    printf("result number: %ld\n", count);
    //set up result
    (*r)->type = POS;
    (*r)->num_tuples = count;
    (*r)->payload = (long*)malloc(sizeof(long)*count); //didnt check allocation error
    memcpy((*r)->payload, temp_result, count*sizeof(long));
    //sort positions
    qsort((*r)->payload, count, sizeof(long), compare_int);
}





//
//investigate
//


/* start of functions for shared scan, assume pre_selected is sorted
 * note shared scan only uses column scan
 * this func scan one column, but compares multiple things
 * and write to multiple results
 * NOTE: array of results have to be allocated already
*/
status shared_select(column *col, result* pre_selected, interval* limits, long length, result* results[]){
    status status_res = {OK, ""};

    //execute each query
    pthread_t tids[NUM_THREAD];
    long size = pre_selected==NULL? col->row_count:pre_selected->num_tuples;
    for (long start = 0; start < size; start+= PAGE_SIZE) {
        long end = start+PAGE_SIZE > size? size:start+PAGE_SIZE;

        for (long i = 0; i < length; ++i) {
            pthread_t *curr_tid = &(tids[i%NUM_THREAD]);
            scan_arg* arg = (scan_arg*)malloc(sizeof(scan_arg));
            arg->col = col;
            arg->lower = limits[i].lower;
            arg->higher = limits[i].upper;
            arg->start = start;
            arg->end = end;
            arg->res = results[i];
            arg->pre_select = pre_selected;
            pthread_create(curr_tid, NULL, col_select_thread, arg);

            //if all threads are used
            if (i%NUM_THREAD == NUM_THREAD-1 || i+1 == length){
                //wait for all process to finish
                for (long j = 0; j<NUM_THREAD; j++){
                    pthread_join(tids[j], NULL);
                }
            }
        }
    }
}

//res should be initialized
void* col_select_thread(void* arg){
    scan_arg* args = (scan_arg*)arg;
    result* res = args->res; //already initialized
    long low = args->lower;
    long high = args->higher;
    long start = args->start;
    long end = args->end;
    column* col = args->col;
    result* pres = args->pre_select;

    pthread_t tid = pthread_self();
    printf("thread id: %d", tid);
    printf("args: l(%ld), h(%ld), s(%ld), t(%ld), res(%p)\n", low, high, start, end, res);

    //scan column (or preselect)
//    int count = 0;
    if (pres){
        for (long i = start; i<end; ++i){
            long pos = pres->payload[i];
            long cur_val = col->data[pos];
            if (cur_val >= low && cur_val < high){
                res->payload[res->num_tuples++] = pos;
            }
        }
    }else{
        for (long i = start; i<end; ++i){
            long cur_val = col->data[i];
            if (cur_val >= low && cur_val < high){
                res->payload[res->num_tuples++] = i;
            }
        }
    }
    free(arg);
}




//start of functions of joins...
/* this func will trigger multi processes for doing nested loop join
 * NOTE!!!!: assumes val1 >= val2 length*/
status nested_loop_join(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2) {
    status res = {OK,""};
    //allocate res1 and res2

    *res1 = (result *) malloc(sizeof(result));
    *res2 = (result *) malloc(sizeof(result));
//    int max_possible = (val1->num_tuples)*(val2->num_tuples);
//    (*res1)->payload = (int*)malloc(sizeof(int)*max_possible);
//    (*res2)->payload = (int*)malloc(sizeof(int)*max_possible);
    (*res1)->type = POS;
    (*res2)->type = POS;
    (*res1)->num_tuples = 0;
    (*res2)->num_tuples = 0;

    //initialize chunks in different threads
    pthread_t tids[NUM_THREAD];
    long* res_pos1s[NUM_THREAD];
    long* res_pos2s[NUM_THREAD];
    long res_lens[NUM_THREAD];

    long len1 = val1->num_tuples;//longer
    long len2 = val2->num_tuples;//shorter

    long chunk_size = len1/NUM_THREAD;

    for (long j=0; j<len2; j+=PAGE_SIZE) {
        //start and end for the shorter column
        long start2 = j;
        long end2 = j+PAGE_SIZE>len2? len2:j+PAGE_SIZE;
        for (long i = 0; i < NUM_THREAD; ++i) {
            long start1 = i * chunk_size;
            long end1 = i==NUM_THREAD-1? len1:start1+chunk_size;
            res_pos1s[i] = (long *) malloc(sizeof(long) * chunk_size * (end2-start2));
            res_pos2s[i] = (long *) malloc(sizeof(long) * chunk_size * (end2-start2));
            join_arg *args = (join_arg *) malloc(sizeof(join_arg));
            args->val1 = val1;
            args->val2 = val2;
            args->start1 = start1;
            args->end1 = end1;
            args->pos1 = pos1;
            args->pos2 = pos2;
            args->start2 = start2;
            args->end2 = end2;
            args->res_len = &(res_lens[i]);
            args->res_pos1 = res_pos1s[i];
            args->res_pos2 = res_pos2s[i];
            pthread_create(&(tids[i]), NULL, nested_loop_join_thread, args);
        }
        for (long i=0;i <NUM_THREAD; ++i){
            pthread_join(tids[i], NULL);
        }
    }
    //get count of valid pairs
    long total_len = 0;
    for (long i=0; i<NUM_THREAD; ++i) total_len+=res_lens[i];
    //allocate space and concat
    (*res1)->payload = (long*)malloc(sizeof(long)*total_len);
    (*res2)->payload = (long*)malloc(sizeof(long)*total_len);

    for (long i = 0; i<NUM_THREAD; ++i){
        for (long j=0; j<res_lens[i]; ++j){
            (*res1)->payload[(*res1)->num_tuples++] = res_pos1s[i][j];
            (*res2)->payload[(*res2)->num_tuples++] = res_pos2s[i][j];
        }
        free(res_pos1s[i]);
        free(res_pos2s[i]);
    }

}

void* nested_loop_join_thread(void* args){
    long count = 0;
    join_arg* arg = (join_arg*)args;
    for (long i = arg->start1; i<arg->end1; ++i){
        for (long j = arg->start2; j<arg->end2; ++j){
            if (arg->val1->payload[i]==arg->val2->payload[j]){
                arg->res_pos1[count] = arg->pos1->payload[i];
                arg->res_pos2[count] = arg->pos2->payload[j];
                ++count;
            }
        }
    }
    *(arg->res_len) = count;
    free(args);
}

//local func for nested loop join
status nested_loop_join_local(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2){
    status res = {OK,""};
    //allocate res1 and res2
    *res1 = (result*)malloc(sizeof(result));
    *res2 = (result*)malloc(sizeof(result));



    //nested loop comparisons
    long n = val1->num_tuples;
    long m = val2->num_tuples;
    long temp_res1[n*m], temp_res2[n*m];
    long count=0;
    for (long i = 0; i<n; ++i){
        for (long j=0; j<n; ++j){
            if (val1->payload[i] == val2->payload[j]){
                temp_res1[count] = pos1->payload[i];
                temp_res2[count] = pos2->payload[j];
                count++;
            }
        }
    }
    //set up results
    (*res1)->type = POS;
    (*res2)->type = POS;
    (*res1)->num_tuples = count;
    (*res2)->num_tuples = count;
    (*res1)->payload = (long*)malloc(sizeof(long)*count);
    (*res2)->payload = (long*)malloc(sizeof(long)*count);
    memcpy((*res1)->payload, temp_res1, count*sizeof(long));
    memcpy((*res2)->payload, temp_res2, count*sizeof(long));
}

//multi-thread hash join
status hash_join(result* val1, result* pos1, result* val2, result* pos2, result** res1, result** res2){
    status return_status = {OK, ""};

    //set up partitions
    long *partition_val1s[NUM_PARTITION];
    long *partition_pos1s[NUM_PARTITION];
    long *partition_val2s[NUM_PARTITION];
    long *partition_pos2s[NUM_PARTITION];
    long count1[NUM_PARTITION] = {0};
    long count2[NUM_PARTITION] = {0};

    //partition both columns
    get_partitions(val1, pos1, partition_val1s, partition_pos1s, count1);
    get_partitions(val2, pos2, partition_val2s, partition_pos2s, count2);

//    printf("Printing partitions for vector 1\n");
//    for (long i = 0; i<NUM_PARTITION; i++){
//        long* temp = partition_val1s[i];
//        long tlen = count1[i];
//        printf("total len: %ld\n", tlen);
//        for (long j = 0; j<tlen; ++j) {
//            printf("%ld ", temp[j]);
//        }
//        printf("\n");
//    }
//    printf("Printing partitions for vector 2\n");
//    for (long i = 0; i<NUM_PARTITION; i++){
//        long* temp = partition_val2s[i];
//        long tlen = count2[i];
//        printf("total len: %ld\n", tlen);
//        for (long j = 0; j<tlen; ++j) {
//            printf("%ld ", temp[j]);
//        }
//        printf("\n");
//    }
//    printf("---------------------\n");


    //do hash join for each partition
    result *valid_pos1[NUM_PARTITION]; //result for each partition (position) from vector 1
    result *valid_pos2[NUM_PARTITION]; //result for each partition (position) from vector 2
    pthread_t tids[NUM_THREAD];

    for (long i = 0; i<NUM_PARTITION; ++i){
        valid_pos1[i] = (result*)malloc(sizeof(result));
        valid_pos2[i] = (result*)malloc(sizeof(result));
        valid_pos1[i]->payload = (long*)malloc(sizeof(long)*(count1[i]*count2[i]));
        valid_pos2[i]->payload = (long*)malloc(sizeof(long)*(count1[i]*count2[i]));
        valid_pos1[i]->num_tuples = 0;
        valid_pos2[i]->num_tuples = 0;
        hashjoin_arg* args = (hashjoin_arg*)malloc(sizeof(hashjoin_arg));
        if (count1[i] <= count2[i]) {
//            printf("%d, %d\n", partition_val1s[i][0], partition_val2s[i][0]);
            args->val1 = partition_val1s[i];
            args->pos1 = partition_pos1s[i];
            args->len1 = count1[i];
            args->val2 = partition_val2s[i];
            args->pos2 = partition_pos2s[i];
            args->len2 = count2[i];
            args->res_pos1 = valid_pos1[i];
            args->res_pos2 = valid_pos2[i];
        }else {
            args->val2 = partition_val1s[i];
            args->pos2 = partition_pos1s[i];
            args->len2 = count1[i];
            args->val1 = partition_val2s[i];
            args->pos1 = partition_pos2s[i];
            args->len1 = count2[i];
            args->res_pos2 = valid_pos1[i];
            args->res_pos1 = valid_pos2[i];
        }

        pthread_create(&(tids[i%NUM_THREAD]), NULL, hash_join_thread, args);
        if (i%NUM_THREAD == (NUM_THREAD-1) || i+1 == NUM_PARTITION){
            for (long j =0 ;j<NUM_THREAD; ++j){
                pthread_join(tids[j], NULL);
            }
        }
//        pthread_join(tids[0], NULL);
//        break;


    }



//    for (int j=0 ;j<NUM_PARTITION; ++j) {
//        printf("partition %d ----\n", j);
//        for (int i = 0; i < valid_pos1[j]->num_tuples; ++i) {
//            printf("(%d,%d)", valid_pos1[j]->payload[i], valid_pos2[j]->payload[i]);
//        }
//        printf("\n");
//    }

    //get count for each partition's join result
    long final_count1 = 0;
    long final_count2 = 0;
    for (long i=0; i<NUM_PARTITION; ++i){
        final_count1 += valid_pos1[i]->num_tuples;
        final_count2 += valid_pos2[i]->num_tuples;
    }
    printf("final count1: %ld\n", final_count1);
    printf("final count2: %ld\n", final_count2);



    //initialize and concat result
    //initialization
    (*res1) = (result*)malloc(sizeof(result));
    (*res2) = (result*)malloc(sizeof(result));
    (*res1)->payload = (long*)malloc(sizeof(long)*final_count1);
    (*res2)->payload = (long*)malloc(sizeof(long)*final_count2);
    (*res1)->num_tuples = 0;
    (*res2)->num_tuples = 0;
    for (long i = 0; i<NUM_PARTITION; ++i){
        for (long j = 0; j<valid_pos1[i]->num_tuples; ++j){
            (*res1)->payload[(*res1)->num_tuples++] = valid_pos1[i]->payload[j];
        }
        for (long j=0; j<valid_pos2[i]->num_tuples; ++j){
            (*res2)->payload[(*res2)->num_tuples++] = valid_pos2[i]->payload[j];
        }
    }
    //free locally allocated space
    for (long i=0; i<NUM_PARTITION; ++i){
        free_result(valid_pos1[i]);
        free_result(valid_pos2[i]);
        free(partition_pos1s[i]);
        free(partition_pos2s[i]);
        free(partition_val1s[i]);
        free(partition_val2s[i]);

    }
}


void* hash_join_thread(void* arg){

    hashjoin_arg* args = (hashjoin_arg*)arg;
    if (args->len1==0 || args->len2 == 0){
        free(args);
        return NULL;
    }

    htable *myhtable = NULL;
    htb_create(&myhtable, args->len1);
    //insert each value from partition 1 to the table
    for (long i = 0; i<args->len1; ++i){
        htb_insert(myhtable, args->val1[i], args->pos1[i]);
    }
    //for each value in partition 2, search in table
    for (long i=0; i<args->len2; ++i){
        htb_node* curr_node = htb_getkey(myhtable, args->val2[i]);
        //if it exists
        if(curr_node != NULL){
            for (long j=0; j<curr_node->curr_len; ++j){
                args->res_pos1->payload[args->res_pos1->num_tuples++] = curr_node->poses[j];
                args->res_pos2->payload[args->res_pos2->num_tuples++] = args->pos2[i];
            }
        }

    }
    free(arg);
    htb_destroy(myhtable);
}

static void get_partitions(result* val, result* pos, long* par_val[NUM_PARTITION], long* par_pos[NUM_PARTITION], long count[NUM_PARTITION]) {
    pthread_t tids[NUM_THREAD];

    long length = val->num_tuples;
    long step_size = length/NUM_THREAD;//size for each thread
    long largest_step = step_size + (length%NUM_THREAD);

    //prepare the write space
    long *part_val_thread[NUM_THREAD][NUM_PARTITION];
    long *part_pos_thread[NUM_THREAD][NUM_PARTITION];
    long len[NUM_THREAD][NUM_PARTITION];
    //initialize
    for (long i=0; i<NUM_THREAD; ++i){
        for (long j=0; j<NUM_PARTITION; ++j){
            part_val_thread[i][j] = (long*)malloc(sizeof(long)*largest_step);
            part_pos_thread[i][j] = (long*)malloc(sizeof(long)*largest_step);
            len[i][j] = 0;
        }
    }
    //multi-process
    for (long j = 0; j<NUM_THREAD; ++j){
        //for each thread, 4 write space for val and pos partitions
        long start = j*step_size;
        long end = j==NUM_THREAD-1? start+largest_step:start+step_size;
        partition_arg* args = (partition_arg*)malloc(sizeof(partition_arg));
        args->len = len[j];
        args->part_val = part_val_thread[j];
        args->part_pos = part_pos_thread[j];
        args->val = val;
        args->pos = pos;
        args->start = start;
        args->end = end;

        pthread_create(&(tids[j]), NULL, partition_thread, args);
    }
    for (long j =0; j<NUM_THREAD; ++j){
        pthread_join(tids[j], NULL);
    }

//    for (int i= 0; i<NUM_PARTITION; ++i) {
//        for (int j = 0; j < len[1][i]; ++j) {
//            printf("%d ", part_val_thread[1][i][j]);
//        }
//        printf("\n");
//    }



    //get final count
    for (long i=0; i<NUM_THREAD; ++i){
        for (long j=0; j<NUM_PARTITION; ++j){
            count[j] += len[i][j];
        }
    }

    //allocate space
    for (long i=0; i<NUM_PARTITION; i++){
        par_val[i] = (long*)malloc(sizeof(long)*count[i]);
        par_pos[i] = (long*)malloc(sizeof(long)*count[i]);
    }

    //merge results
    long ks[NUM_PARTITION] = {0};
    for (long i=0; i<NUM_THREAD; ++i) {
        for (long j=0; j<NUM_PARTITION; ++j) {
            for (long k=0; k<len[i][j]; k++){
                par_val[j][ks[j]] = part_val_thread[i][j][k];
                par_pos[j][ks[j]++] = part_pos_thread[i][j][k];
            }
        }
    }

//    for (int i = 0; i<NUM_PARTITION; ++i){
//        for (int j=0; j<count[i]; ++j){
//            printf("%d ", par_val[i][j]);
//        }
//        printf("\n");
//    }

    //free local allocated space
    for (long i=0; i<NUM_THREAD; ++i){
        for (long j=0; j<NUM_PARTITION; ++j){
            free(part_val_thread[i][j]);
            free(part_pos_thread[i][j]);
        }
    }
}

void* partition_thread(void* arg){
    partition_arg* args = (partition_arg*)arg;
//    printf("id %d, %p\n", pthread_self(), args->len);
//    printf("start %d, end %d\n", args->start, args->end);
    for (long i = args->start; i<args->end; ++i){
        long curr_val = args->val->payload[i];
        long curr_pos = args->pos->payload[i];
        long curr_part = curr_val%NUM_PARTITION;
        args->part_val[curr_part][(args->len[curr_part])] = curr_val;
        args->part_pos[curr_part][(args->len[curr_part])++] = curr_pos;
    }

//    for (int i = 0; i<NUM_PARTITION; ++i){
//        for (int j=0; j<args->len[i]; ++j){
//            printf("%d ", args->part_val[i][j]);
//        }
//        printf("\n");
//    }

    free(arg);
}





/*Updates and aggregates*/
status add(result* val1, result* val2, result** res_val){
    long len = val1->num_tuples;

    (*res_val) = (result*)malloc(sizeof(result));
    (*res_val)->payload = (long*)malloc(sizeof(long)*len);
    (*res_val)->type = VAL;
    (*res_val)->num_tuples = len;

    for (long i = 0; i<len; ++i){
        (*res_val)->payload[i] = val1->payload[i]+val2->payload[i];
    }
}

//assume val1 - val2
status subtract(result* val1, result* val2, result** res_val){
    long len = val1->num_tuples;

    (*res_val) = (result*)malloc(sizeof(result));
    (*res_val)->payload = (long*)malloc(sizeof(long)*len);
    (*res_val)->type = VAL;
    (*res_val)->num_tuples = len;

    for (long i = 0; i<len; ++i){
        (*res_val)->payload[i] = val1->payload[i]-val2->payload[i];
    }
}


//if min_pos==NULL, dont get min pos
status min(result* val, result* pos, result** min_val, result** min_pos){

    *min_val = (result*)malloc(sizeof(result));
    (*min_val)->payload = (long*)malloc(sizeof(long));
    (*min_val)->type = VAL;
    (*min_val)->num_tuples = 1;

    if (pos){
        (*min_pos) = (result*)malloc(sizeof(result));
        (*min_pos)->payload = (long*)malloc(sizeof(long));
        (*min_pos)->type = POS;
        (*min_pos)->num_tuples = 1;
    }


    long min_idx = 0;
    long min_so_far = val->payload[0];
    for (long i=0; i<val->num_tuples; ++i){
        if (val->payload[i]<min_so_far){
            min_so_far = val->payload[i];
            min_idx = pos==NULL? i:pos->payload[i];
        }
    }


    (*min_val)->payload[0] = min_so_far;
    if (pos) (*min_pos)->payload[0] = min_idx;

}

//if max_pos==NULL, dont get max pos
status max(result* val, result* pos, result** max_val, result** max_pos){
    *max_val = (result*)malloc(sizeof(result));
    (*max_val)->payload = (long*)malloc(sizeof(long));
    (*max_val)->type = VAL;
    (*max_val)->num_tuples = 1;

    if (pos){
        (*max_pos) = (result*)malloc(sizeof(result));
        (*max_pos)->payload = (long*)malloc(sizeof(long));
        (*max_pos)->type = POS;
        (*max_pos)->num_tuples = 1;
    }

    long max_idx = 0;
    long max_so_far = val->payload[0];
    for (long i=0; i<val->num_tuples; ++i){
        if (val->payload[i]>max_so_far){
            max_so_far = val->payload[i];
            max_idx = pos==NULL? i:pos->payload[i];
        }
    }

    //assign to result
    (*max_val)->payload[0] = max_so_far;
    if (pos) (*max_pos)->payload[0] = max_idx;
}

//NOTE: didnt do error checking
status avg(result* val, result** avg_val){
    (*avg_val) = (result*)malloc(sizeof(result));
    (*avg_val)->payload = (long*)malloc(sizeof(long));
    (*avg_val)->num_tuples = 1;
    (*avg_val)->type = VAL;

    //find sum
    double sum = 0;
    for (long i=0; i<val->num_tuples; ++i){
        sum += ((val->payload[i])/(double)(val->num_tuples));
    }

    (*avg_val)->payload[0] = (long)sum;
}

//NOTE: assume pos is sorted
status update(column *col, result* pos, long new_val){
    for (long i=0;i <pos->num_tuples; ++i){
        col->data[pos->payload[i]] = new_val;
    }
}

status tuple(result* res_arr[], long num_res, char** tuple){
    long len = res_arr[0]->num_tuples;
    char buffer[len*num_res*20];
    strcpy(buffer, "");

    for (long i = 0; i<len; ++i){
        for (long j=0 ; j<num_res; ++j){
            long cur = res_arr[j]->payload[i];
            char temp[20];
            sprintf(temp, "%ld", cur);
            strcat(buffer,temp);
            if (j!=num_res-1) strcat(buffer,",");
        }
        strcat(buffer,"\n");
    }
    *tuple = strdup(buffer);
}


// debugging print functions
void print_db_table(){
    printf("------------------------------------\n");
    printf("printing database name table...\n");
    db_node* temp = db_table;
    long i = 0;
    while (temp) {
        printf("%ld: %s\n", i++, temp->this_db->name);
        temp = temp->next;
    }
}

void print_db(db* database){
    printf("------------------------------------\n");
    printf("printing database...\n");
    printf("db name: %s | tbl count: %ld\n", database->name, database->table_count);
    for (long i =0 ; i<database->db_size; ++i){
        if (database->tables_pos[i] == FULL){
            printf("\t at [%ld]: [%s]\n", i, database->tables[i].name);
        }
    }
}

void print_tbl(table* tbl, long print_data){
    printf("------------------------------------\n");
    printf("printing table...\n");
    printf("tbl name: %s | max col: %ld | curr count: %zu \n", tbl->name, tbl->tb_size, tbl->col_count);
    long rows = 0;
    printf("\t");
    for (long i = 0; i<tbl->tb_size; ++i){
        if (tbl->cols_pos[i] == FULL) {
            rows = tbl->cols[i].row_count;
            printf("| [%s] |", tbl->cols[i].name);
        }
    }
    printf("\n");
    printf("total rows: %ld\n", rows);
    if (print_data) {
        for (long i = 0; i < rows; ++i) {
            printf("\t");
            for (long j = 0; j < tbl->tb_size; ++j) {
                if (tbl->cols_pos[j] == FULL) {
                    printf("| %ld |", tbl->cols[j].data[i]);
                }
            }
            printf("\n");
        }
    }
}

void print_system(long print_data){
    printf("------------------------------------\n");
    printf("printing entire system...\n");
    db_node* temp = db_table;
    while (temp){
        db* cur_db = temp->this_db;
        print_db(cur_db);
        for (long i = 0; i<cur_db->db_size; i++){
            if (cur_db->tables_pos[i] == FULL){
                print_tbl(&(cur_db->tables[i]), print_data);
            }
        }
        temp = temp->next;
    }
}

void print_result(result* res){
    printf("------------------------------------\n");
    printf("result type: %s\n", res->type==POS? "positions":"values");
    for (long i = 0; i<res->num_tuples; i++){
        printf("%ld,", res->payload[i]);
    }
    printf("\n");
}

//free things
void free_result(result* res){
    if (res){
        if (res->num_tuples) {
            free(res->payload);
            res->payload = NULL;
        }
        free(res);
        res = NULL;
    }
}

// ---------start of not finished..------------------


//---------------update functions-------------------//

//status update(column *col, int *pos, int new_val){
//
//}
//
//
//
//}
//
///* Query API */
//status query_prepare(const char* query, db_operator** op);
//status query_execute(db_operator* op, result** results);
//
//
int main(){
    db* mydb = NULL;
    create_db("db1", &mydb);
    open_db("../project_tests/data4.csv", &mydb);
    open_db("../project_tests/data5.csv", &mydb);

    table* tbl4 = &(mydb->tables[0]);
    table* tbl5 = &(mydb->tables[1]);

    create_clustered_index(tbl4, &(tbl4->cols[6]));
    create_clustered_index(tbl5, &(tbl5->cols[6]));

    result *s1, *s2, *f1, *f2, *r1, *r2;
    col_select_local(&(tbl4->cols[0]), 20000, 40000, &s1, NULL);
    col_select_local(&(tbl5->cols[0]), 30000, 70000, &s2, NULL);
    fetch(&(tbl4->cols[0]), s1, &f1);
    fetch(&(tbl5->cols[0]), s2, &f2);
    printf("%ld, %ld\n", f1->num_tuples, f2->num_tuples);

//    hash_join(s1,f1,s2,f2,&r1,&r2);


//    result *f3, *f4;
//    fetch(&(tbl4->cols[0]), r1, &f3);
//    fetch(&(tbl5->cols[0]), r2, &f4);
//    result* res[2];
//    res[0] = f3;
//    res[1] = f4;

//    char* haha;
//    tuple(res, 2, &haha);
//    printf("%s\n", haha);


}

//int main() {
//    db *mydb = NULL;
//    create_db("db1", &mydb);
//    open_db("./data.txt", &mydb);
//    open_db("../project_tests/data1.csv", &mydb);
//    table* tbl1 = &(mydb->tables[0]);
//    table* tbl2 = &(mydb->tables[1]);
//
//    relational_insert(tbl2,"-1,-11,-111,-1111,-11111,1,11");
//    relational_insert(tbl2,"-2,-22,-222,-2222,-11111,1,11");
//    relational_insert(tbl2,"-3,-33,-333,-2222,-11111,1,11");
//    relational_insert(tbl2,"-4,-44,-444,-2222,-11111,1,11");
//    relational_insert(tbl2,"-5,-55,-555,-2222,-15111,1,11");
////    open_db("../project_tests/data2.csv", &mydb);
//
//
////    create_clustered_index(&(mydb->tables[1]),&(mydb->tables[1].cols[6]));
//    create_index(&(tbl2->cols[0]), B_PLUS_TREE);
////    create_index(&(tbl2->cols[1]), B_PLUS_TREE);
//    result* s1=NULL, *f1=NULL;
////    col_select_local(&(tbl2->cols[0]), 220000000,330000000, &s1, NULL);
//    btree_select_local(&(tbl2->cols[0]), 220000000,330000000, &s1, NULL);
//
//    fetch(&(tbl2->cols[4]), s1, &f1);
//    print_result(f1);


//    relational_insert(&(mydb->tables[0]),"-1,-11,-111,-1111,-11111,1,11,111,1111,11111");
//    relational_insert(&(mydb->tables[0]),"-2,-22,-222,-2222,-11111,1,11,111,1111,11511");
//    relational_insert(&(mydb->tables[0]),"-3,-33,-333,-2222,-11111,1,11,111,1911,11111");
//    relational_insert(&(mydb->tables[0]),"-4,-44,-444,-2222,-11111,1,11,111,1111,11511");
//    relational_insert(&(mydb->tables[0]),"-5,-55,-555,-2222,-15111,1,11,111,1811,11111");
//    relational_insert(&(mydb->tables[0]),"-6,-66,-666,-2222,-11111,1,16,111,1111,13411");
//    relational_insert(&(mydb->tables[0]),"-7,-77,-777,-2222,-11111,1,11,711,1111,11111");
//    relational_insert(&(mydb->tables[0]),"-8,-88,-888,-2222,-18101,1,11,111,8111,11211");
//    relational_insert(&(mydb->tables[0]),"-9,-99,-999,-2222,-91141,1,11,111,1111,16711");
//    relational_insert(&(mydb->tables[0]),"-10,-11,0,-34,-11111,1,11,111,1111,11111");
//    result *s1=NULL, *f1=NULL, *a1=NULL;
//    col_select_local(&(mydb->tables[0].cols[6]), 127, 1234567890, &s1, NULL);
//    fetch(&(mydb->tables[0].cols[4]), s1, &f1);
//    avg(f1, &a1);
//    print_result(a1);

//}
//    result* res=NULL, *res2 = NULL, *res3=NULL;
//    col_select_local(&(mydb->tables[0].cols[0]), -200000000,0,&res,NULL);
//    fetch(&(mydb->tables[0].cols[0]), res, &res2);
//    print_result(res2);
////
//    prepare_close_conn(DATA_PATH, 1);
//    prepare_open_conn(DATA_PATH);
//    print_db(db_table->this_db);
//    fetch(&(db_table->this_db->tables[0].cols[0]), res, &res3);
//    print_result(res3);

//    print_db(mydb);
//    print_tbl(&(mydb->tables[0]), 0);
//    relational_insert(&(mydb->tables[0]),"-1,-11,-111,-1111,-11111,1,11,111,1111,11111");
//    prepare_close_conn(DATA_PATH);
//    result* res=NULL, *res2 = NULL, *res3=NULL;
//    col_select_local(&(mydb->tables[0].cols[0]), -200000000,0,&res,NULL);
//    fetch(&(mydb->tables[0].cols[0]), res, &res2);
//    fetch(&(mydb->tables[0].cols[1]), res, &res3);
//
//    result* res_arr[2];
//    res_arr[0] = res2;
//    res_arr[1] = res3;
//
//    char* string;
//    tuple(res_arr, 2, &string);
////    print_result(res);
//    printf("%s\n", string);

//    return 0;
//}


//
//int main(){
//    db* mydb = NULL;
//    create_db("mydb", &mydb);
//    table* tb = NULL;
//    column* col1 = NULL;
//    column* col2 = NULL;
//    create_table(mydb, "tb", 2, &tb);
//    create_column(tb, "col1", &col1);
//    create_column(tb, "col2", &col2);
//    open_db("./tt", &mydb);
//    print_db(mydb);
//
////
////
//////    int length = 3;
//////    result* results[3];
//////    //initialize result and payload
//////    for (int i = 0; i<length; i++){
//////        results[i] = (result*)malloc(sizeof(result));
//////        results[i]->type = POS;
//////        results[i]->payload = (int*)malloc(sizeof(int)*col1->row_count);
//////        results[i]->num_tuples = 0;
//////    }
//////    interval limits[3] = {{0,-1},{10,15},{1,5}};
//////    shared_select(col1, NULL, limits, length, results);
//////
//////    print_result(results[0]);
//////    print_result(results[1]);
//////    print_result(results[2]);
////
////    open_db("./test_data", &mydb);
////    column* col1 = &(mydb->tables[0].cols[0]);
////    column* col2 = &(mydb->tables[0].cols[1]);
////    column* col3 = &(mydb->tables[1].cols[0]);
////    column* col4 = &(mydb->tables[1].cols[1]);
////
//////    create_index(col1, B_PLUS_TREE);
//////    result* vec_pos = NULL, *vec_val = NULL;
//////    print_tbl(&(mydb->tables[0]), 1);
//////    col_select(col1, 0, 10, &vec_pos, NULL);
//////    fetch(col2, vec_pos->payload, vec_pos->num_tuples, &vec_val);
//////    print_result(vec_val);
//////
//////    index_select(col1, 0, 10, &vec_pos);
//////    fetch(col2, vec_pos->payload, vec_pos->num_tuples, &vec_val);
//////    print_result(vec_val);
////
////
////    result *pos1=NULL, *pos2=NULL, *val1=NULL, *val2=NULL, *respos1=NULL, *respos2=NULL;
////    col_select_local(col2, 90, 100, &pos1, NULL); //selecting grades in [95,100]
////    col_select_local(col4, 165, 166, &pos2, NULL); //selecting course == 165
////    fetch(col1, pos1->payload, pos1->num_tuples, &val1); //fetch student id with grades [95,100]
////    fetch(col3, pos2->payload, pos2->num_tuples, &val2); //fetch student id with course 165
////
////
//////    nested_loop_join_local(val1, pos1, val2, pos2, &respos1, &respos2);
////    hash_join(val1, pos1, val2, pos2, &respos1, &respos2);
////    result *t1=NULL, *t2= NULL;
////    fetch(col1, respos1->payload, respos1->num_tuples, &t1);
////    fetch(col3, respos2->payload, respos2->num_tuples, &t2);
////    print_result(t1);
////    print_result(t2);
////
////
////
////
//////    free_result(respos1);free_result(respos2);respos1 = NULL; respos2=NULL;
//////    nested_loop_join(val1, pos1, val2, pos2, &respos1, &respos2);
//////    free_result(t1);free_result(t2);t1 = NULL;t2 = NULL;
//////    fetch(col1, respos1->payload, respos1->num_tuples, &t1);
//////    fetch(col3, respos2->payload, respos2->num_tuples, &t2);
//////    print_result(t1);
//////    print_result(t2);
////
////    free_result(pos1);
////    free_result(pos2);
////    free_result(val1);
////    free_result(val2);
////    free_result(respos1);
////    free_result(respos2);
////    free_result(t1);
////    free_result(t2);
////    drop_db(mydb, 0, 1);
//
//
//
//
//    return 0;
//}