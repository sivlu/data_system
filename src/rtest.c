#include "./include/cs165_api.h"
#include <time.h>
#include <limits.h>

int main(){
    time_t start, end;
    db* mydb = NULL;
    result* temp = NULL;
    //load files
    create_db("db1", &mydb);
    open_db("mydata1.txt", &mydb);
    open_db("mydata2.txt", &mydb);
    open_db("mydata3.txt", &mydb);
    open_db("mydata4.txt", &mydb);
    open_db("mydata_join.txt", &mydb);

    //get cols
    column* col1 = &(mydb->tables[0].cols[0]); // 1000 col
    column* col2 = &(mydb->tables[1].cols[0]); //10000 col
    column* col3 = &(mydb->tables[2].cols[0]); //100000 col
    column* col4 = &(mydb->tables[3].cols[0]); //1000000 col






    //put selectivity limits
    int a[] = {268,520,769};
    int b[] = {2405,4946,7506};
    int c[] = {24893, 49974,75043};
    int d[] = {249651,500243,750048};

//    /***** COLUMN SELECT on 1 MILLION DATA********************/
//    //create btree index for all of them
//    create_index(col1, B_PLUS_TREE);
//    create_index(col2, B_PLUS_TREE);
//    create_index(col3, B_PLUS_TREE);
//    create_index(col4, B_PLUS_TREE);
//
//    start = clock();
//    col_select_local(col4, a[0],a[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity column select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//    start = clock();
//    col_select_local(col4, a[0],a[2], &temp, NULL);
//    end = clock();
//    printf("50 selectivity column select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//    start = clock();
//    col_select_local(col4, INT_MIN,a[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity column select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//    start = clock();
//    col_select_local(col4, INT_MIN, INT_MAX, &temp, NULL);
//    end = clock();
//    printf("100 selectivity column select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);


    /***** SORTED SELECT on 1 MILLION DATA********************/
//    create_index(col4, SORTED);


//    start = clock();
//    sorted_select_local(col4, d[0],d[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity sorted select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    start = clock();
//    sorted_select_local(col4, d[0],d[2], &temp, NULL);
//    end = clock();
//    printf("50 selectivity sorted select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    start = clock();
//    sorted_select_local(col4, INT_MIN,d[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity sorted select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);

//    start = clock();
//    sorted_select_local(col4, INT_MIN, INT_MAX, &temp, NULL);
//    end = clock();
//    printf("100 selectivity sorted select (1 million rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);



//    /********** 25 SELECTIVITY *****************/
//    //1,000 rows
//    start = clock();
//    btree_select_local(col1, a[0],a[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity btree select (1000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //10,000 rows
//    start = clock();
//    btree_select_local(col2, b[0], b[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //100,000 rows
//    start = clock();
//    btree_select_local(col3, c[0], c[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //1,000,000 rows
//    start = clock();
//    btree_select_local(col4, d[0], d[1], &temp, NULL);
//    end = clock();
//    printf("25 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//
//
//    /********** 50 SELECTIVITY *****************/
//    //1,000 rows
//    start = clock();
//    btree_select_local(col1, 268, 769, &temp, NULL);
//    end = clock();
//    printf("50 selectivity btree select (1000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //10,000 rows
//    start = clock();
//    btree_select_local(col2, 2405, 7506, &temp, NULL);
//    end = clock();
//    printf("50 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //100,000 rows
//    start = clock();
//    btree_select_local(col3, 24893, 75043, &temp, NULL);
//    end = clock();
//    printf("50 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //1,000,000 rows
//    start = clock();
//    btree_select_local(col4, 249651, 750058, &temp, NULL);
//    end = clock();
//    printf("50 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//
//
//    /********** 75 SELECTIVITY *****************/
//    //1,000 rows
//    start = clock();
//    btree_select_local(col1, INT_MIN ,a[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity btree select (1000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //10,000 rows
//    start = clock();
//    btree_select_local(col2, INT_MIN, b[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //100,000 rows
//    start = clock();
//    btree_select_local(col3, INT_MIN, c[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);
//
//    //1,000,000 rows
//    start = clock();
//    btree_select_local(col4, INT_MIN, d[2], &temp, NULL);
//    end = clock();
//    printf("75 selectivity btree select (10000 rows): %.4f\n", (end-start)/(float)CLOCKS_PER_SEC);
//    free_result(temp);



    //TODO: Change NODESIZE



};