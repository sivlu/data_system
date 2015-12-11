//
// Created by Siv Lu on 12/6/15.
//

#include "./include/hashtable.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void htb_create(htable** mytable, long length){
    (*mytable) = (htable*)malloc(sizeof(htable));
    (*mytable)->length = length;
    (*mytable)->content = (htb_node*)malloc(sizeof(htb_node)*length);
    (*mytable)->slot = (long*)malloc(sizeof(long)*length);
    for (long i=0; i<length; ++i){
        (*mytable)->slot[i] = 1;
    }
}
void htb_insert(htable* mytable, long key, long pos){
    long idx = calculate_hash(mytable, key);
    if (mytable->slot[idx]){
        mytable->slot[idx] = 0;
        htb_node* temp = &(mytable->content[idx]);
        init_htb_node(temp, key, pos);
    }else{
        htb_node* temp = &(mytable->content[idx]);
        htb_node* prev = NULL;
        //see if val already exist in the linked list
        while (temp){
            if (temp->value == key) break;
            prev = temp;
            temp = temp->next;
        }
        //if found, append to pos list
        if (temp){
            //check if pos array is full
            if (temp->curr_len == temp->max_len){
                long* t = temp->poses;
                temp->poses = (long*)malloc(sizeof(long)*temp->max_len*2);
                memcpy(temp->poses, t, temp->max_len* sizeof(long));
                temp->max_len = (temp->max_len)*2;
            }
            temp->poses[temp->curr_len++] = pos;
        }else{
            //append a new node
            htb_node* new_entry = (htb_node*)malloc(sizeof(htb_node));
            init_htb_node(new_entry, key, pos);
            prev->next = new_entry;
        }
    }
}

htb_node* htb_getkey(htable* mytable, long key){
    long idx = calculate_hash(mytable, key);
    //if idx is empty
    if (mytable->slot[idx] == 1) return NULL;
    //search that linked list
    htb_node* temp = &(mytable->content[idx]);
    while (temp){
        if (temp->value == key) break;
        temp = temp->next;
    }
    return temp;
}

void htb_destroy(htable* mytable){
    if (!mytable) return;

    printf("table len: %ld\n", mytable->length);
    for (long i = 0; i<mytable->length; ++i){
        if (mytable->slot[i] == 0){
            destroy_htb_node(&(mytable->content[i]));
            free(mytable->content[i].poses);
        }
    }
    free(mytable->content);
    free(mytable->slot);
    free(mytable);
}

static void destroy_htb_node(htb_node* node){
    htb_node* temp = node->next;
    while (temp) {
        if (temp->poses) free(temp->poses);
        htb_node* t = temp;
        temp = temp->next;
        free(t);
    }
}

static void init_htb_node(htb_node* node, long key, long pos){
    node->value = key;
    node->curr_len = 1;
    node->max_len = NUM_DUP;
    node->poses = (long*)malloc(sizeof(long)*NUM_DUP);
    (node->poses)[0] = pos;
    node->next = NULL;
}

static long calculate_hash(htable* mytable, long val){
    if (val<0) return (-val)%(mytable->length);
    return val%(mytable->length);
}

static void print_table(htable* mytable){
    for (long i=0; i<mytable->length; ++i){
        if (mytable->slot[i] == 0){
            htb_node* curr = &(mytable->content[i]);
            while (curr) {
                printf("(%ld)", curr->value);
                for (long j = 0; j < curr->curr_len; ++j) {
                    printf("%ld,", curr->poses[j]);
                }
                printf("|");
                curr = curr->next;
            }
            printf("\n");
        }
    }
}
//int main(){
//    htable* mytable = NULL;
//    htb_create(&mytable, 20);
//    int keys[] = {0,-3,24,15,6,2,5,2};
//    int vals[] = {1,2,3,4,5,6,7,8};
////    int keys[] = {1,2,21,15,33,2,2,2};
////    int vals[] = {3,4,2,1,6,7,3,2};
//    int len = 8;
//    for (int i=0; i<len; ++i){
//        htb_insert(mytable, keys[i], vals[i]);
//    }
////    print_table(mytable);
////    printf("found %d: %d\n", 1, htb_getkey(mytable, 1)!=NULL);
////    printf("found %d: %d\n", 5, htb_getkey(mytable, 5)!=NULL);
//////    //testing poses
////    htb_node* temp = htb_getkey(mytable, 2);
////    for (int i=0; i<temp->curr_len; ++i){
////        printf("%d, ", temp->poses[i]);
////    }
////    temp = htb_getkey(mytable, 0);
////    for (int i=0; i<temp->curr_len; ++i){
////        printf("%d, ", temp->poses[i]);
////    }
//    htb_destroy(mytable);
//}
