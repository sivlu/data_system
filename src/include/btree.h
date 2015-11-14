//
// Created by Siv Lu on 11/8/15.
//

#ifndef BLUS_CS165_2015_BASE_BTREE_H
#define BLUS_CS165_2015_BASE_BTREE_H

#include "data_structure.h"
#include "utils.h"

#define NODESIZE 3



typedef struct node {
    void* ptr[NODESIZE+1];
    int val[NODESIZE];
    int is_leaf;
    int node_size;
}b_node;

void insert_btree(b_node** head, int value);
b_node* search_btree(b_node* head, int target);
void destroy_btree(b_node* head);
void print_btree(b_node* head);

#endif //BLUS_CS165_2015_BASE_BTREE_H

