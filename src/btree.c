//
// Created by Siv Lu on 11/8/15.
//

#include "./include/btree.h"

void insert_btree(btree_node** head, int value, int pos){
    insert_btree_helper(head, *head, value, pos);
}

static void init_node(btree_node** head, int is_leaf){
    *head = (btree_node*)malloc(sizeof(btree_node));
    (*head)->is_leaf = is_leaf;
    (*head)->node_size = NODESIZE;
    (*head)->num = 0;
    (*head)->parent = NULL;
    for (int i=0; i<NODESIZE; i++){
        (*head)->val[i] = 0;
        (*head)->ptr[i] = NULL;
    }
    //fix last pointer in leaf
    (*head)->ptr[NODESIZE] = NULL;
}

static void insert_btree_helper(btree_node** head, btree_node* curr, int value, int pos){
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

static void split_insert_leaf(btree_node** head, btree_node* leaf, int value, int pos){
    //initialize leaf
    btree_node* new_leaf = NULL;
    init_node(&new_leaf, 1);

    //split node
    split_node(leaf, new_leaf, 1);

    //insert new value to one leaf node
    int mid = leaf->val[NODESIZE/2];
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

static void insert_node(btree_node** head, btree_node* parent, btree_node* target, int value){
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
        int mid = parent->val[NODESIZE/2];
        if (value < mid){
            simple_insert_node(parent,target,value);
        }else{
            simple_insert_node(new_parent, target, value);
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

static void fix_head(btree_node** head, int val, btree_node* left, btree_node* right){
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
static void simple_insert_node(btree_node* parent, btree_node* new_node, int value){
    new_node->parent = parent;

    int slot = 0;
    for (;slot<parent->num; slot++){
        if (value < parent->val[slot]) break;
    }
    //special case, append at end
    if (slot == parent->num){
        parent->val[parent->num] = value;
        parent->ptr[parent->num+1] = new_node;
        parent->num++;
    }else {
        //rearrange ptr and vals
        parent->val[parent->num] = parent->val[slot];
        parent->val[slot] = value;
        parent->ptr[parent->num + 1] = parent->ptr[parent->num];
        parent->ptr[parent->num] = new_node;
        parent->num++;
        //keep swapping
        int num = parent->num - 1;
        slot++;
        while (slot != parent->num) {
            int t_int = parent->val[slot];
            void *t_ptr = parent->ptr[slot];
            parent->val[slot] = parent->val[num];
            parent->ptr[slot] = parent->ptr[num];
            parent->val[num] = t_int;
            parent->ptr[num] = t_ptr;
            slot++;
        }
    }

}

//simple insert to a definitely not full leaf node
static void simple_insert_leaf(btree_node* node, int value, int pos){
    //assign value, increment count
    node->val[node->num] = value;
    if (!node->ptr[node->num])
        node->ptr[node->num] = (int*)malloc(sizeof(int));
    *(int*)(node->ptr[node->num]) = pos;
    node->num++;
    //sort node if more than 1
    if (node->num > 1){
        //sort the values
        key_val pairs[node->num];
        for (int i = 0; i<node->num; ++i){
            pairs[i].key = node->val[i];
            pairs[i].val = node->ptr[i];
        }
        qsort(pairs, node->num, sizeof(key_val), compare_key_val);
        for (int i = 0; i<node->num; ++i){
            node->val[i] = pairs[i].key;
            node->ptr[i] = pairs[i].val;
        }
    }
}

//left is full node, right is empty node
static void split_node(btree_node* left, btree_node* right, int is_leaf){
    //fix key-val pairs
    int start = is_leaf? NODESIZE/2:NODESIZE/2+1;
    for (int i = start; i<NODESIZE; i++){
        right->val[i-start] = left->val[i];
        if (is_leaf) {
            if (!right->ptr[i-start])
                right->ptr[i-start] = (int*)malloc(sizeof(int));
            *(int *) (right->ptr[i - start]) = *(int *) (left->ptr[i]);
        }
        else
            right->ptr[i-start] = left->ptr[i];
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
        //in left, num-- since mid is going to be pushed up
        left->num--;
    }
}



//return node with range that target falls into
btree_node* search_btree(btree_node* head, int target){
    if (head->is_leaf){
        return head;
    }else{
        for (int i=0;i<head->num;i++){
            if (target<head->val[i]) {
                return search_btree((btree_node *) head->ptr[i], target);
            }
        }
        return search_btree((btree_node*) head->ptr[head->num], target);
    }
}

void destroy_btree(btree_node* head){
    int count = 0;
    btree_node* queue[COL_SIZE*2];
    traverse_btree(head, &count, queue);
    for (int i = 0; i<count; ++i){
        free_node(queue[i]);
    }
    free_node(head);
}

static void free_node(btree_node* node){
    if (node->is_leaf) {
        for (int i = 0; i < NODESIZE; ++i) {
            if (node->ptr[i]) free(node->ptr[i]);
        }
    }
    free(node);
}

static void traverse_btree(btree_node* curr, int* count, btree_node* queue[]){
    if (!curr->is_leaf){
        for (int i = 0; i<curr->num+1;++i){
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
        for (int i = 0; i< head->num; i++){
            printf("%d ", (head->val[i]));
        }
        printf("\n");
        for (int i = 0; i < head->num + 1; i++) {
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
    for (int i = 0; i<leaf->num; ++i){
        printf("%d ", leaf->val[i]);
    }
    printf(" | ");
}

static int compare_key_val(const void* a, const void *b){
    key_val *p1 = (key_val*)a, *p2 = (key_val*)b;
    if (p1->key < p2->key) return -1;
    if (p1->key == p2->key) return 0;
    return 1;

}

//int main(){
//    btree_node* head = NULL;
//    int top = 4;
//    int data[] = {8,9,0,1,2,3,4,5,6,7};
//    for (int i = 0; i<top; ++i){
//        insert_btree(&head, data[i], i);
//    }
//    print_btree(head);
//    print_leaf_level(head);
////    btree_node* temp = search_btree(head, 7);
////    print_leaf_node(temp);
//    destroy_btree(head);
//    return 0;
//}