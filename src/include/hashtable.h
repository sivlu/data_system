//
// Created by Siv Lu on 12/6/15.
//

#ifndef BLUS_CS165_2015_BASE_HASHTABLE_H
#define BLUS_CS165_2015_BASE_HASHTABLE_H

#define NUM_DUP 5 //default for poses array in htb_pos_list

typedef struct htb_node{
    long value;
    long* poses; //pos of the save value
    long curr_len; //number of poses, used for resizing the array
    long max_len;
    struct htb_node* next;
}htb_node;


typedef struct htable{
    htb_node* content; //ARRAY of nodes in tables
    long* slot; //ARRAY of binaries to indicate whether one slot is taken
    long length; //table length
}htable;

void htb_create(htable** mytable, long length);
void htb_insert(htable* mytable, long key, long pos);
htb_node* htb_getkey(htable* mytable, long key);
void htb_destroy(htable* mytable);

static void init_htb_node(htb_node* node, long key, long pos);
static long calculate_hash(htable* mytable, long val);
static void destroy_htb_node(htb_node* node);
static void prlong_table(htable* mytable);
#endif //BLUS_CS165_2015_BASE_HASHTABLE_H
