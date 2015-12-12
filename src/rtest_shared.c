#include "./include/cs165_api.h"
#include <time.h>
#include <limits.h>

#define NQUERY 500
#define ROWS 1000000

int main() {
    time_t start, end;
    db *mydb = NULL;
    result *temp = NULL;
    //load files
    create_db("db1", &mydb);
    open_db("mydata4.txt", &mydb);

    //get col
    column *col = &(mydb->tables[0].cols[0]); //1000000 col

    //creates limits
    int lows[100] = {173984, 212825, 136990, 202054, 659700, 253547, 270910, 310357, 460103, 125251, 437102, 622724, 359927, 292132, 126591, 873894, 407487, 785437, 344083, 359596, 712178, 33706, 625745, 287558, 61658, 371050, 394846, 559774, 156581, 817647, 682039, 486522, 408305, 104282, 180606, 111397, 168430, 686791, 631284, 230574, 308400, 134503, 441167, 952947, 129329, 346124, 254814, 268630, 36791, 320375, 554380, 378671, 308354, 221695, 497915, 192582, 160612, 153732, 93185, 171088, 227569, 424530, 918, 282200, 177706, 716055, 85074, 110758, 344364, 181861, 599667, 292379, 450361, 694224, 853239, 283550, 158270, 679373, 546934, 776066, 391221, 102155, 356232, 143075, 577951, 310599, 326880, 79718, 155872, 346531, 132128, 301827, 658380, 166592, 172598, 643354, 12040, 179276, 75389, 231776};
    int highs[100] = {687023, 537453, 454240, 945409, 695024, 549995, 640200, 735597, 517171, 250796, 592406, 667350, 527184, 775758, 335296, 998489, 660751, 865661, 794986, 586969, 841866, 570695, 771665, 845458, 204772, 846877, 680385, 942267, 442516, 937295, 797271, 850350, 950070, 675690, 696772, 168681, 736871, 966784, 735972, 482475, 602808, 399507, 578318, 985854, 239430, 723015, 487705, 889453, 211569, 781050, 730135, 571786, 417806, 999276, 938076, 861389, 310995, 302497, 93316, 886947, 236826, 819700, 864315, 973930, 684832, 738621, 576932, 190555, 532788, 552272, 721663, 422713, 884708, 797751, 885022, 760388, 397834, 943706, 939336, 962770, 964513, 117324, 604948, 983332, 999393, 745306, 473784, 595620, 977085, 479135, 212095, 392357, 803026, 874966, 464582, 962773, 107437, 950633, 102086, 233699};

    //prepare for shared scan
    interval limits[NQUERY];
    result* results[NQUERY];
    for (int i = 0; i < NQUERY; ++i) {
        limits[i].lower = lows[i%NQUERY];
        limits[i].upper = highs[i%NQUERY];
        results[i] = (result *) malloc(sizeof(result));
        results[i]->num_tuples = 0;
        results[i]->type = POS;
    }


    //get timer
    start = clock();
    shared_select(col, NULL, limits, NQUERY, results);
    end = clock();
    printf("Running %d queries in parallel: %.4f\n", NQUERY, (end-start)/(float)CLOCKS_PER_SEC);

    //free
    for (int i = 0; i<NQUERY; ++i){
        free_result(results[i]);
    }

//    start = clock();
//    for (int i = 0; i < NQUERY; ++i) {
//        col_select_local(col, limits[i%100].lower, limits[i%100].upper, &(results[i]), NULL);
//    }
//    end = clock();
//    printf("Running %d queries sequentially: %.4f\n", NQUERY, (end-start)/(float)CLOCKS_PER_SEC);
//
//    //free
//    for (int i = 0; i<NQUERY; ++i){
//        free_result(results[i]);
//    }


}