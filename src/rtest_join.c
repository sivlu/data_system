#include "./include/cs165_api.h"
#include <time.h>
#include <limits.h>

int main() {
    time_t start, end;
    db *mydb = NULL;
    result *temp = NULL;
    //load files
    create_db("db1", &mydb);
    open_db("mydata1.txt", &mydb);
    open_db("mydata2.txt", &mydb);
    open_db("mydata3.txt", &mydb);
    open_db("mydata4.txt", &mydb);
    open_db("mydata_join.txt", &mydb);

    //get cols
    column *col1 = &(mydb->tables[0].cols[0]); // 1000 col
    column *col2 = &(mydb->tables[1].cols[0]); //10000 col
    column *col3 = &(mydb->tables[2].cols[0]); //100000 col
    column *col4 = &(mydb->tables[3].cols[0]); //1000000 col
    column *col_join = &(mydb->tables[4].cols[0]); //join col

    //select all
    result *scol1, *scol2, *scol3, *scol4, *scol_join;
    col_select_local(col1,INT_MIN,INT_MAX,&scol1,NULL);
    col_select_local(col2,INT_MIN,INT_MAX,&scol2,NULL);
    col_select_local(col3,INT_MIN,INT_MAX,&scol3,NULL);
    col_select_local(col4,INT_MIN,INT_MAX,&scol4,NULL);
    col_select_local(col_join,INT_MIN,INT_MAX,&scol_join,NULL);

//    //try multi-core Nested Loop join
//    start = clock();
//    result* r1, *r2;
//    nested_loop_join(scol1,scol1,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop join on 1,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join(scol2,scol2,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop join on 10,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join(scol3,scol3,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop join on 100,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join(scol4,scol4,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop join on 1,000,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);

//    //LOCAL JOINS
//    start = clock();
//    result* r1, *r2;
//    nested_loop_join_local(scol1,scol1,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop (local) join on 1,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join_local(scol2,scol2,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop (local) join on 10,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join_local(scol3,scol3,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop (local) join on 100,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);
//
//    free_result(r1);
//    free_result(r2);
//    start = clock();
//    nested_loop_join_local(scol4,scol4,scol_join,scol_join,&r1,&r2);
//    end = clock();
//    printf("Nested loop (local) join on 1,000,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);

    //multi-core hash JOINS
    start = clock();
    result* r1, *r2;
    hash_join(scol1,scol1,scol_join,scol_join,&r1,&r2);
    end = clock();
    printf("Hash join on 1,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);

    free_result(r1);
    free_result(r2);
    start = clock();
    hash_join(scol2,scol2,scol_join,scol_join,&r1,&r2);
    end = clock();
    printf("Hash join on 10,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);

    free_result(r1);
    free_result(r2);
    start = clock();
    hash_join(scol3,scol3,scol_join,scol_join,&r1,&r2);
    end = clock();
    printf("Hash join on 100,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);

    free_result(r1);
    free_result(r2);
    start = clock();
    hash_join(scol4,scol4,scol_join,scol_join,&r1,&r2);
    end = clock();
    printf("Hash join on 1,000,000: %.4f\n",(end-start)/(float)CLOCKS_PER_SEC);


}