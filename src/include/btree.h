//
// Created by Siv Lu on 11/8/15.
//

#ifndef BLUS_CS165_2015_BASE_BTREE_H
#define BLUS_CS165_2015_BASE_BTREE_H

#include "data_structure.h"
#include "utils.h"

#define NODESIZE 3



//NOTE: ptr on the left denotes <, on the right denotes >=
typedef struct node {
    void* ptr[NODESIZE+1]; //k+1 pointers
    int val[NODESIZE]; //k keys
    int is_leaf; //indicate whether it's leaf node
    int node_size; //max size
    int num; //current number of key/values
    struct node* parent; //parent node
}btree_node;

typedef struct key_val{
    int key;
    void* val;
}key_val;

typedef struct pos_node{
    int pos;
    struct pos_node *next;
}pos_node;


void insert_btree(btree_node** head, int value, int pos);
btree_node* search_btree(btree_node* head, int target);
void destroy_btree(btree_node* head);
void print_btree(btree_node* head);
void print_leaf_node(btree_node* leaf);
void print_leaf_level(btree_node* head);


//static funcs
/*compare func for key-val pairs*/
static int compare_key_val(const void* a, const void *b);
/*init a btree node*/
static void init_node(btree_node** head, int is_leaf);
/*insert to a leaf node without splitting*/
static void simple_insert_leaf(btree_node* node, int value, int pos);
/*insert to a non-leaf node without splitting*/
static void simple_insert_node(btree_node* parent, btree_node* new_node, int value);
/*helper func for insert_btree*/
static void insert_btree_helper(btree_node** head, btree_node* curr, int value, int pos);
/*split the leaf node and insert the value*/
static void split_insert_leaf(btree_node** head, btree_node* leaf, int value, int pos);
/*general func for insert to a non-leaf node*/
static void insert_node(btree_node** head, btree_node* parent, btree_node* new_node, int value);
/*simply split the left (full) node, and copy half of it into right (empty) node*/
static void split_node(btree_node* left, btree_node* right, int is_leaf);
/*fix root of tree by creating new node and fix pointers and first value*/
static void fix_head(btree_node** head, int val, btree_node* left, btree_node* right);
/*free allocated space in a node*/
static void free_node(btree_node* node);
/*traverse and append node to queue*/
static void traverse_btree(btree_node* curr, int* count, btree_node* queue[]);


#endif //BLUS_CS165_2015_BASE_BTREE_H

