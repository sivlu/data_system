//
// Created by Siv Lu on 12/6/15.
//

#ifndef BLUS_CS165_2015_BASE_HASHTABLE_H
#define BLUS_CS165_2015_BASE_HASHTABLE_H

#define NUM_DUP 5 //default for poses array in htb_pos_list

typedef struct htb_node{
    int value;
    int* poses; //pos of the save value
    int curr_len; //number of poses, used for resizing the array
    int max_len;
    struct htb_node* next;
}htb_node;


typedef struct htable{
    htb_node* content; //ARRAY of nodes in tables
    int* slot; //ARRAY of binaries to indicate whether one slot is taken
    int length; //table length
}htable;

void htb_create(htable** mytable, int length);
void htb_insert(htable* mytable, int key, int pos);
htb_node* htb_getkey(htable* mytable, int key);
void htb_destroy(htable* mytable);

static void init_htb_node(htb_node* node, int key, int pos);
static int calculate_hash(htable* mytable, int val);
static void destroy_htb_node(htb_node* node);
static void print_table(htable* mytable);
#endif //BLUS_CS165_2015_BASE_HASHTABLE_H
