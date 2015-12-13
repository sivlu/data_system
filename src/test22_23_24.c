#include "./include/cs165_api.h"
#include <limits.h>

#define NQUERY 10

int main(){
    // create db
    db* mydb = NULL;
    create_db("db1", &mydb);
    //load file [load(../project_tests/data2.csv)]
    open_db("../project_tests/data2.csv", &mydb);
    //get table and cols ptrs
    table* mytbl = &(mydb->tables[0]);
    column* col1 = &(mydb->tables[0].cols[0]);
    column* col2 = &(mydb->tables[0].cols[1]);
    column* col3 = &(mydb->tables[0].cols[2]);
    column* col4 = &(mydb->tables[0].cols[3]);
    column* col5 = &(mydb->tables[0].cols[4]);
    column* col6 = &(mydb->tables[0].cols[5]);
    column* col7 = &(mydb->tables[0].cols[6]);
    //cluster on col4 [create(idx,db1.tbl3.col4,clustered)]
    create_clustered_index(mytbl, col4);
    //sorted index on col1 [create(idx,db1.tbl3.col1,sorted)]
    create_index(col1, SORTED);
    //btree index on col3 [create(idx,db1.tbl3.col3,btree)]
    create_index(col3, B_PLUS_TREE);
    //btree with clustered col
    create_index(col4, B_PLUS_TREE);

    int n_cols = (int)mytbl->tb_size;

    //--------START OF TEST 22----------------
    result** s[n_cols]; //list of result array
    result* f[NQUERY]; //result array

    interval* limits[n_cols];
    int count [n_cols];
    for (int i = 0; i<n_cols; ++i) count[i] = 0;


    //THINGS I NEED TO INITIALIZE STUFF--------------------//
//    int bounds[] = {78091301,118548668,153472534,190489750,-250003324,-153542055,360161038,430400334,
//                  174964847,234227183,-367021856,-284434020,113326821,141646368,441002272,488716814,
//                  -185636195,-119462531,84546226,172851319};
//    column* cols[] = {col4, col6,col3,col1,col2,col5,col7,col1,col7,col1};
//    column* f_cols[]={col4, col6,col4,col7,col3,col5,col7,col1,col3,col4};

    int bounds[] = {78091301,118548668,153472534,190489750,-250003324,-153542055,360161038,430400334,174964847,234227183,-367021856,-284434020,113326821,141646368,441002272,488716814,-185636195,-119462531,84546226,172851319,-87129792,-59360427,157890939,222902219,67235477,120298588,275512701,307758138,447693925,505186288,-225296608,-154002650,-44265141,-6894806,277511390,338521394,-494991426,-416548436,-420074968,-361470774,369545546,445047037,410533818,474542045,-10613883,48994212,84935049,150335078,-58653183,38997682,-497169574,-432013190,-391243588,-332187713,-41359716,16378595,226215201,318560817,149529118,242744663,119112000,195638496,329423194,390343915,-64643317,24770102,-453764114,-360281715,481319360,527730790,95132764,189101240,-420254323,-380655100,499698038,591263085,195236794,259420890,55519873,100461610,127128892,167848424,371337450,445257198,-410977715,-381833981,259399505,331562621,6506961,69571682,-158474130,-78983150,-22991906,38001447,301096974,356491240,492349412,571398582,336456972,362680808,240442123,311215095,-331469608,-299310616,-280123662,-199255210,71894452,129842903,-483951887,-443852342,22819317,108555336,-173301758,-144727706,65179224,154659451,-328569839,-291739661,499837966,527173370,-150644332,-95838232,-354491026,-264573770,222559031,321510974,322304307,373055394,-305663742,-248196424,-312536623,-239995514,293598100,345612415,-448162272,-399839264,-323256760,-250542801,447384849,475592549,-154662112,-115042289,252345823,312462825,-326893763,-235919881,13355759,105103269,459738708,493617104,-189161728,-142414286,-154417725,-61376565,32526875,58443401,193408990,225676112,76962210,131897012,468813389,507951011,-425133621,-339292028,-161502296,-67605868,125330981,224335358,-210798428,-124248746,114703242,183546410,-468904197,-443494567,270723639,369159775,-485371660,-427823180,401555762,467932624,-40895399,9290713,-11173457,67027144,-234686440,-196480879,-421423727,-357056300,-379615260,-301710639,75792799,148433333,9759189,88237605,146309601,211477802,191299214,247717378,169353766,203021296};
    column* cols[] = {col4,col6,col3,col1,col2,col5,col7,col1,col7,col1,col2,col6,col2,col1,col6,col2,col1,col6,col4,col3,col3,col4,col1,col3,col2,col1,col1,col4,col4,col3,col2,col5,col7,col7,col7,col1,col3,col1,col6,col5,col5,col7,col3,col4,col5,col4,col4,col6,col7,col6,col3,col2,col7,col2,col7,col5,col7,col1,col5,col7,col4,col3,col5,col6,col4,col7,col7,col1,col1,col2,col4,col3,col7,col3,col1,col6,col7,col5,col5,col4,col3,col4,col6,col3,col2,col2,col4,col1,col3,col5,col3,col5,col1,col2,col1,col6,col3,col5,col3,col3};
    column* f_cols[] = {col4,col6,col4,col7,col3,col5,col7,col1,col3,col4,col2,col4,col6,col5,col2,col3,col2,col5,col3,col3,col5,col2,col5,col3,col4,col3,col1,col7,col4,col5,col2,col5,col6,col6,col6,col5,col2,col2,col4,col3,col3,col5,col1,col2,col2,col2,col4,col5,col6,col1,col3,col2,col6,col7,col7,col6,col6,col5,col3,col5,col7,col5,col5,col2,col4,col2,col4,col3,col7,col3,col5,col2,col6,col5,col3,col1,col5,col5,col2,col5,col2,col7,col1,col6,col2,col2,col3,col1,col1,col5,col1,col1,col3,col4,col1,col6,col6,col5,col2,col7};



    //initialize db operators, and count how many queries on each column
    db_operator* ops[NQUERY];
    for (int i=0; i<NQUERY; ++i){
        f[i] = NULL;
        ops[i] = (db_operator*)malloc(sizeof(db_operator));
        ops[i]->low = bounds[i*2];
        ops[i]->high = bounds[i*2+1];
        ops[i]->col = cols[i];
        for (int j = 0; j<mytbl->tb_size; ++j){
            if (&(mytbl->cols[j]) == cols[i]){
                count[j] += 1;
            }
        }
    }
    //prepare the argument (result array, limits array)
    for (int i = 0; i<n_cols; ++i){
        column* this_col = &(mytbl->cols[i]);
        interval** this_limit = &(limits[i]);
        result*** this_res_arr = &(s[i]);
        (*this_limit) = (interval*)malloc(sizeof(interval)*count[i]);
        (*this_res_arr) = (result**)malloc(sizeof(result*)*count[i]);

        for (int j=0; j<count[i]; ++j){
            //TODO: Instead of malloc, assign the correct variable here
            (*this_res_arr)[j] = (result*)malloc(sizeof(result));
        }

        int k = 0;
        for (int j=0;j<NQUERY;++j){
            if (ops[j]->col == this_col){
                (*this_limit)[k].lower = ops[j]->low;
                (*this_limit)[k++].upper = ops[j]->high;
            }
        }

    }

    //call shared scan
    for (int i=0; i<n_cols; ++i){
        printf("sharing [%d] queries on col [%d]\n", count[i], i);
        column* this_col = &(mytbl->cols[i]);
        if (count[i] == 1){
            //SINGLE SELECT TAKES A NULL RESULT AND ALLOCATE THERE
            result** this_res = s[i];

            if (this_col->index && this_col->index->type==SORTED){
                sorted_select_local(this_col, limits[i][0].lower, limits[i][0].upper, this_res, NULL);
            }else if (this_col->index && this_col->index->type==B_PLUS_TREE){
                btree_select_local(this_col, limits[i][0].lower, limits[i][0].upper, this_res, NULL);
            }else{
                col_select_local(this_col, limits[i][0].lower, limits[i][0].upper, this_res, NULL);
            }
        }else if (count[i] > 1){
            shared_select(this_col,NULL,limits[i],count[i], s[i]);
        }
    }

    for (int i=0; i<n_cols; ++i){
        column* select_col = &(mytbl->cols[i]);
        result** res_arr = s[i];

        int k=0;
        for (int j =0; j<NQUERY; ++j){
            if (cols[j] == select_col){
                result* temp = NULL;
                fetch(f_cols[j], res_arr[k++], &temp);
                printf("------------------------------------\n");
                printf("Results for Query [%d]\n", j+1);
                print_result(temp);
                free(temp);
            }
        }
    }





}
