#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>

//#endif




typedef struct haha{
    int a;
    int b;
}HAHA;

int main(){
    char* temp = NULL;
    char kk[] = "adsfasf";
    char* token = strtok(kk, "p");
    token=strtok(NULL,"p");
    temp=strdup(token);
    printf("%s\n", temp);
    free(temp);
}
