//
// Created by Siv Lu on 11/8/15.
//

#include "./include/btree.h"

void insert_btree(btree_node** head, long value, long pos){
//    printf("%ld,", value);
    insert_btree_helper(head, *head, value, pos);
}

static void init_node(btree_node** head, long is_leaf){
    *head = (btree_node*)malloc(sizeof(btree_node));
    (*head)->is_leaf = is_leaf;
    (*head)->node_size = NODESIZE;
    (*head)->num = 0;
    (*head)->parent = NULL;
    for (long i=0; i<NODESIZE; i++){
        (*head)->val[i] = 0;
        (*head)->ptr[i] = NULL;
    }
    //fix last pointer in leaf
    (*head)->ptr[NODESIZE] = NULL;
}

static void insert_btree_helper(btree_node** head, btree_node* curr, long value, long pos){
    if (!*head){
        init_node(head, 1);
        simple_insert_leaf(*head, value, pos);
    }else if (curr->is_leaf) {
        if (curr->num < NODESIZE) {
            //leaf node and leaf not full
            simple_insert_leaf(curr, value, pos);
        }else{
            //leaf is full, split
            split_insert_leaf(head, curr, value, pos);
        }
    }else{
        //not leaf node, recursive call
        if (value < curr->val[0]){
            btree_node* temp = (btree_node*)curr->ptr[0];
            insert_btree_helper(head, temp, value, pos);
        }else {
            for (int i = curr->num - 1; i >= 0; --i) {
                if (value >= curr->val[i]) {
                    btree_node* temp = (btree_node*)curr->ptr[i+1];
                    insert_btree_helper(head, temp, value, pos);
                    break;
                }
            }
        }
    }
}

static void split_insert_leaf(btree_node** head, btree_node* leaf, long value, long pos){
    //initialize leaf
    btree_node* new_leaf = NULL;
    init_node(&new_leaf, 1);
    long mid = leaf->val[NODESIZE/2];


    //split node
    split_node(leaf, new_leaf, 1);

    //insert new value to one leaf node
    if (value >= mid){
        simple_insert_leaf(new_leaf, value, pos);
    }else{
        simple_insert_leaf(leaf, value, pos);
    }

    //fix parent
    if (*head == leaf){
        //only 1 level (leaf level)
        fix_head(head, mid, leaf, new_leaf);
    }else{
        btree_node* parent = leaf->parent;
        insert_node(head, parent, new_leaf, mid);
    }
}

static void insert_node(btree_node** head, btree_node* parent, btree_node* target, long value){
    if (parent->num < NODESIZE){
        //target parent node is not full, simple insert
        simple_insert_node(parent, target, value);
    }else{
        //recursively split and insert
        //split parent node first
        btree_node* new_parent = NULL;
        init_node(&new_parent, 0);
        split_node(parent, new_parent, 0);
        //figure out which one to insert
        long mid = parent->val[NODESIZE/2];
        if (value < mid){
            simple_insert_node(parent,target,value);
        }else{
            simple_insert_node(new_parent,target,value);
        }
        //fix parent's parent
        if (*head == parent){
            //special case: parent is root of tree
            fix_head(head, mid, parent, new_parent);
        }else{
            btree_node* grand_parent = parent->parent;
            insert_node(head, grand_parent, new_parent, mid);
        }
    }
}

static void fix_head(btree_node** head, long val, btree_node* left, btree_node* right){
    btree_node* temp = NULL;
    init_node(&temp, 0);
    temp->val[0] = val;
    temp->ptr[0] = left;
    temp->ptr[1] = right;
    temp->num = 1;
    left->parent = temp;
    right->parent = temp;
    *head = temp;
}

//parent is not full
static void simple_insert_node(btree_node* parent, btree_node* new_node, long value){
    //new_node is at right ptr of value
    new_node->parent = parent;

    int slot = 0;
    for (slot=0; slot<parent->num; slot++){
        if (value < parent->val[slot]) break;
    }
    //special case, append at end
    if (slot == parent->num){
        parent->val[parent->num] = value;
        parent->ptr[parent->num+1] = new_node;
        parent->num++;
    }else {
        //move last ptr 1-slot right
        parent->ptr[parent->num + 1] = parent->ptr[parent->num];
        //move new node at num
        parent->ptr[parent->num] = new_node;

        //move things on [slot] to [num]
        parent->val[parent->num] = parent->val[slot];
//        parent->ptr[parent->num] = parent->ptr[slot];
        //put value and new node on [slot]
        parent->val[slot] = value;
        //increment parent's count
        parent->num++;

        //keep swapping until slot and num meet
        int num = parent->num - 1;
        slot = slot + 1;
        while (slot != num) {
            long t_long = parent->val[slot];
            void *t_ptr = parent->ptr[slot];
            parent->val[slot] = parent->val[num];
            parent->ptr[slot] = parent->ptr[num];
            parent->val[num] = t_long;
            parent->ptr[num] = t_ptr;
            slot++;
        }
    }

}

//simple insert to a definitely not full leaf node
static void simple_insert_leaf(btree_node* node, long value, long pos){
    //search if this value is already there
    for (long i = 0; i<node->num; ++i){
        if (node->val[i] == value){
            pos_node* temp = (pos_node*)node->ptr[i];
            while (temp->next) temp = temp->next;
            pos_node* new_pos = (pos_node*)malloc(sizeof(pos_node));
            new_pos->pos = pos;
            new_pos->next = NULL;
            temp->next = new_pos;
            return;
        }
    }

    //if value not in there
    //assign value, increment count
    node->val[node->num] = value;
    node->ptr[node->num] = (pos_node*)malloc(sizeof(pos_node));
    ((pos_node*)(node->ptr[node->num]))->pos = pos;
    ((pos_node*)(node->ptr[node->num]))->next = NULL;
    node->num++;


    //sort node if more than 1
    if (node->num > 1){
        //sort the values
        key_val pairs[node->num];
        for (long i = 0; i<node->num; ++i){
            pairs[i].key = node->val[i];
            pairs[i].val = node->ptr[i];
        }
        qsort(pairs, node->num, sizeof(key_val), compare_key_val);
        for (long i = 0; i<node->num; ++i){
            node->val[i] = pairs[i].key;
            node->ptr[i] = pairs[i].val;
        }
    }


}

//left is full node, right is empty node
static void split_node(btree_node* left, btree_node* right, long is_leaf){
    //fix key-val pairs
    int start = is_leaf? NODESIZE/2:NODESIZE/2+1;
//    int start = NODESIZE/2;

    for (int i = start; i<NODESIZE; i++){
        right->val[i-start] = left->val[i];
        if (is_leaf) {
//            if (!right->ptr[i-start]) right->ptr[i-start] = (pos_node*)malloc(sizeof(pos_node));
            right->ptr[i - start] = left->ptr[i];
            left->ptr[i] = NULL;
        }
        else {
            right->ptr[i - start] = left->ptr[i];
            btree_node* temp = (btree_node*)right->ptr[i-start];
            temp->parent = right;
        }
        left->num--;
        right->num++;
    }

    //fix last pointer
    if (is_leaf) {
        //fix linked list on leaf level
        right->ptr[NODESIZE] = left->ptr[NODESIZE];
        left->ptr[NODESIZE] = right;
    }else{
        //in right, ptr of lst left ptr should be copied
        right->ptr[right->num] = left->ptr[NODESIZE];
        btree_node* temp = (btree_node*)right->ptr[right->num];
        temp->parent = right;
        //in left, num-- since mid is going to be pushed up
        left->num--;
    }
}



//return node with range that target falls into
btree_node* search_btree(btree_node* head, long target){
    if (head->is_leaf){
        return head;
    }else{
        for (int i=0; i<head->num; i++){
            if (target < head->val[i]) {
                return search_btree((btree_node*)head->ptr[i], target);
            }
        }
        return search_btree((btree_node*) head->ptr[head->num], target);
    }
}

void destroy_btree(btree_node* head){
    long count = 0;
    btree_node* queue[COL_SIZE*2];
    traverse_btree(head, &count, queue);
    for (long i = 0; i<count; ++i){
        free_node(queue[i]);
    }
    free_node(head);
}

static void free_node(btree_node* node){
    if (node->is_leaf) {
        for (long i = 0; i < NODESIZE; ++i) {
            if (node->ptr[i]){
                pos_node* temp = (pos_node*)node->ptr[i];
                while (temp){
                    pos_node* t = temp->next;
                    free(temp);
                    temp = t;
                }
            }
        }
    }
    free(node);
}

static void traverse_btree(btree_node* curr, long* count, btree_node* queue[]){
    if (!curr->is_leaf){
        for (long i = 0; i<curr->num+1;++i){
            btree_node* temp = (btree_node*)curr->ptr[i];
            queue[(*count)++] = temp;
            traverse_btree(temp,count, queue);
        }
    }
}

void print_btree(btree_node* head){
    if (!head) return;
    if (head->is_leaf) {
        printf("leaf node, count: %d ", head->num);
        print_leaf_node(head);
        printf("\n");
    }else {
        printf("non-leaf node, count: %d, keys: ", head->num);
        for (long i = 0; i< head->num; i++){
            printf("%ld ", (head->val[i]));
        }
        printf("\n");
        for (long i = 0; i < head->num + 1; i++) {
            print_btree((btree_node *) head->ptr[i]);
        }
    }
}

void print_leaf_level(btree_node* head){
    btree_node* temp = head;
    while (!temp->is_leaf){
        temp = (btree_node*)temp->ptr[0];
    }
    while (temp){
        print_leaf_node(temp);
        temp = (btree_node*)temp->ptr[NODESIZE];
    }
    printf("\n");
}

void print_leaf_node(btree_node* leaf){
    printf(" | ");
    for (long i = 0; i<leaf->num; ++i){
        printf("%ld(", leaf->val[i]);
        pos_node* temp = (pos_node*)leaf->ptr[i];
        while(temp){
            printf("%ld,",temp->pos);
            temp = temp->next;
        }
        printf(") ");
    }
    printf(" | ");
}

static int compare_key_val(const void* a, const void *b){
    key_val *p1 = (key_val*)a, *p2 = (key_val*)b;
    if (p1->key < p2->key) return -1;
    if (p1->key == p2->key) return 0;
    return 1;

}
//


//int main(){
//    btree_node* head = NULL;
//    int top =500;
//    int data[] = {1225415925,-1685366041,808827558,1953956868,1139015841,1406749897,1258726918,-114769120,2140114637,-2089052137,-654822472,-1064760115,319124257,-268209992,-1624412342,-1538148919,2115574502,864936141,1945328255,995087821,-439099666,2037532462,-1798038786,-1712731026,612432450,1211392358,312753248,-2011573025,1309447495,262924683,-1521745912,-1846596577,-1382556826,-527187017,-1051804079,-907558170,761537789,686995399,1498568319,-827030365,-1904791683,-1894792977,1311288941,-1472583062,-778410213,182708050,807224603,1507893191,-1214825171,742109118,-1668754998,-1702692783,1255314063,301601912,854389942,-57544200,-123130145,-181463807,-719718649,-214234154,-584103481,1358156049,-1507812084,-1952044764,-2009783453,-1640090395,68289647,1671153324,-1364022228,1360934438,-1999661402,1629863485,-268681844,1125556365,-160626562,-251880430,308438066,-58494459,-739874302,466846608,-551635698,1564461027,1730955654,-1798004065,296728904,-84059576,-1446518647,-35637541,-1559894816,704612725,-845588632,6300220,354656159,187431319,-411954430,-2084434120,1209163067,681754850,1569528063,1021977896,-850475375,542122533,-936936602,204492272,-1559367250,-1708533337,180018569,565840013,-1124226435,1309276928,-1935932739,-1438462179,-1000224073,727276573,-1610650222,-660272380,95462316,-889482608,1453724467,2058721564,-1777306390,841908198,1001658567,-364650985,1465382537,-1823511225,-1435261128,357592019,-1519128370,666352182,-1039808542,-1966328515,609477786,1948456321,1178818738,-210611613,1505739663,1196510804,374344425,546198990,2058926685,-1999262683,1101099772,-457101068,407307915,2084953762,746334863,1024894981,601284960,1933211701,-1524039592,-352495860,394688312,114490153,1130641568,-2114763228,-351867054,-676366720,-339157848,368643594,-504199480,-2121158061,-650502764,706579585,750368327,-434640018,512541493,951331145,-913740742,809705718,720546786,822585894,248271881,-121966568,2078957318,-1868851417,-852938559,-1056898697,1337240714,1347301282,52445978,-1845099345,-611164960,-1209209059,-1331051610,-388874935,1095496195,1292738676,1970760206,1110679428,1069905991,2084439035,1220216680,-2069660622,1654679229,1306264039,-980145137,1037417826,1160613470,1915253142,998695637,665039721,1429282025,845573118,-291670049,-1371246433,1648012152,1595486170,-1223208559,1074050804,781410218,632461359,2093406498,82866872,654580834,-767287495,-562007336,991938790,1593164283,-469470446,-1304656115,-627461498,-290619736,-2051302599,-288449902,1375611330,-643354873,1835510649,-61639148,594142702,-1286615184,1621887302,1555587247,-414865335,1352792747,501039408,436037126,-1057810293,-177832888,867920287,1813295417,-1618398946,-1810173833,1680676849,-1803488402,-1156432816,-406817881,-2004032014,34921773,-1661116452,1729389633,-1873050438,1802872495,-1464485385,1358246766,493380240,-1287477060,-2020536984,1509245708,1925373671,352584861,294125412,-748824084,-435810701,1312972119,1248453061,2082742516,-992931746,862559980,-780267079,-1936575005,-1434586526,1683544003,-1346051440,-173651217,-1805048552,-1597955506,-951444467,1030346422,-1654947580,-1867146626,1144718404,-641542772,719903524,-1307678532,1454505904,1803623116,-2116736935,2139150880,711571791,-1012289834,684864290,-762410964,-2016812138,759316997,992468861,-173174456,-939556352,236891161,-820445268,-666115520,-871184804,-1854768038,-568936351,1519357482,1965850029,-1222075610,-1357613629,-1292487139,-1300427781,-750585424,-1651609760,-2076867786,-1753810630,1086607907,974437595,1442754002,323948008,-874120870,728562328,-947448059,-789315641,530300642,-375620087,-17305508,2096082265,1386732773,1718426302,-1926826781,753134472,-2000619333,-113347209,960114958,-359341794,2089563218,1849027124,-333915193,1924330540,-1351791107,-350289776,1070284563,232146450,-328281858,1989045963,187412401,140361724,-1010543227,1343860711,-1635084764,-1272729739,-1532439693,-223008908,1244104597,221901619,1134231363,-1937058266,866217492,287291107,188333509,-237265456,1941946428,-1776107435,1849860761,-1833101395,-231290078,-153203766,292848025,-976882192,2034829649,873035949,-329773276,-872744122,124226902,1158104305,-1338994543,1284406668,2033416177,770317076,-57238300,-1455129919,1115849156,400354855,696184105,1988967798,289969223,388228426,-1230029746,-1800892040,-511151865,-2144953867,-1058850478,-513570840,1034775475,-1422531935,891520552,1011010727,586166460,-855690998,-619657096,-1590982265,1168678697,400522529,-1222325520,1876745348,1034858857,-1333280012,-1238651600,-1865688534,1999402078,514133266,192357355,171189153,-1602023103,-931261399,730712649,-1032910181,-1306204913,-1229428507,-715619761,-1629272559,-1719227900,1404514510,739238881,-383830347,76132190,407205062,2087436172,-437273889,-956227116,494795511,-405361312,-729443383,-667654852,1512223745,-660099589,503011980,-226460832,-405423660,-761013615,1473118910,1937407326,182835226,-347461933,-568725360,926229650,-576672715,-173330457,883858807,-846503316,1851543519,902765916,273336963,-1802368809,-710240644,1072402873,-476886948,-141393016,1214765367,2146151251,227952747,1311809395,1872188275,-972286812,1374503659,-1684184082,1581191126,495789085,349137741,-1174865045,1404396568,582815118,-274287286,1380297456,387055230,98312786,-1795594153,1569578072,515145761,1755128262,-1531296792,-803261254,-451761019,1279498669,72430938,2080161338,-1035983761,1453479651,-2014405699,736764087,1608626560,-864295547,1023089491,1957816593,1669211416,-1073274367,1874199653,-1098325155,962511441,446290345,-1739307692};
////    long data[] = {1406,1258,-1147,2140,-2089,
////                   -6548,-1064,3191,-2682,-1624};
////    long data[] = {8,5,9,3,1,6,7,10,2,4,11};
//
//    for (int i = 0; i<top; ++i){
////        insert_btree(&head, -i, i);
//        insert_btree(&head, data[i], i);
//    }
//
////    print_btree(head);
////    print_leaf_level(head);
//    btree_node* temp = search_btree(head, 220000000);
//    while (temp->val[0] < 330000000){
//        print_leaf_node(temp);
//        temp = (btree_node*)temp->ptr[NODESIZE];
//    }
////    destroy_btree(head);
//    return 0;
//}