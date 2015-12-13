#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

//#endif



int main(){
    char* temp = "dhjfgwegkwegkwefbkwebfiwgefuwe";
    char* chunks[5];
    for (int i=0; i<5; ++i){
        chunks[i] = (char*)malloc(sizeof(char)*6);
        strncpy(chunks[i], temp+i*5, 5);
        chunks[i][5]=0;
    }
    for (int i=0; i<5; ++i){
        printf("%s\n", chunks[i]);
    }

}
